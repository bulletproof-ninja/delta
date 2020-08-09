package delta.process

import delta.SnapshotStore

import collection.Map
import concurrent._
import concurrent.duration._
import scala.annotation.tailrec

import scuff._
import scuff.concurrent._

/**
 * A `StreamProcessStore` keeps track of how far a stream
 * has been processed, and stores any necessary state derived
 * from that stream.
 *
 * @note For state type `S`, consider using a type that is
 * optimized for reading (e.g. a plain JSON `String`) as this makes
 * it easily available as a [[delta.read.ReadModel]] with mostly just
 * pushing bytes. In such cases, the `adapt` methods can be used to
 * accomodate the actual processing setup.
 *
 * @tparam SID The stream id type
 * @tparam S The state type.
 * @tparam U The update type. For small datasets, this is often
 * just the same as `S`. But for larger datasets, it can be a
 * diff'ed format.
 */
trait StreamProcessStore[SID, S, U]
extends SnapshotStore[SID, S] {

  type StreamId = SID
  type Tick = delta.Tick
  type Revision = delta.Revision

  def name: String

  override def toString() = s"${getClass.getSimpleName}($name)"

  type Update = delta.process.Update[U]

  def readBatch(keys: Iterable[SID]): Future[Map[SID, Snapshot]]
  def writeBatch(batch: Map[SID, Snapshot]): Future[Unit]
  def refresh(key: SID, revision: Revision, tick: Tick): Future[Unit]
  def refreshBatch(revisions: Map[SID, (Int, Long)]): Future[Unit]

  /**
   *  Tick watermark. This is a best-effort
   *  watermark, usually either the highest
   *  or last processed. Combined with the
   *  tick window, this is used to
   *  determine where to resume any interrupted
   *  stream processing.
   */
  def tickWatermark: Option[Tick]

  /**
   * Update/Insert.
   * @param key The key to update
   * @param updateThunk The update function. Return `None` if no insert/update is desired
   * @param updateContext Execution context for running the update thunk
   * @return The result of the `updateThunk` function
   */
  def upsert[R](key: SID)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)]

  /** Adapt `S` state type for processing. */
  def adaptState[W](stateCodec: Codec[W, S]): StreamProcessStore[SID, W, U] =
    adapt(Codec.noop, AsyncCodec(stateCodec), Threads.PiggyBack)

  /** Adapt `S` state type for processing. */
  def adaptState[W](stateCodec: AsyncCodec[W, S], asyncContext: ExecutionContext)
      : StreamProcessStore[SID, W, U] =
    adapt(Codec.noop, stateCodec, asyncContext)

  /** Adapt `SID` key/id type for processing. */
  def adaptKeys[W](keyCodec: Codec[W, SID])
      : StreamProcessStore[W, S, U] =
    adapt(keyCodec, AsyncCodec.noop, Threads.PiggyBack)

  /** Adapt `SID` key/id and `S` state types for processing. */
  def adapt[WK, W](
      keyCodec: Codec[WK, SID],
      stateCodec: AsyncCodec[W, S],
      asyncContext: ExecutionContext): StreamProcessStore[WK, W, U] =
    new StreamProcessStoreAdapter[WK, W, SID, S, U](this, keyCodec, stateCodec, asyncContext)

}

/**
 * Trait supporting side-effecting stream processing.
 *
 * @note Side-effects are always at-least-once semantics,
 * so ensure idempotent behavior if possible.
 */
trait SideEffect[SID, S <: AnyRef, U]
extends StreamProcessStore[SID, S, U] {

  /** Perform side-effect, if necessary, and return final state. */
  protected def doSideEffect(state: S): Future[S]

  @inline
  private def replaceContent(snapshot: Snapshot, newState: S): Snapshot =
    if (snapshot.state eq newState) snapshot
    else snapshot.copy(state = newState)

  final abstract override def upsert[R](key: SID)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)] = {
    super.upsert(key) { optSnapshot =>
      updateThunk(optSnapshot).flatMap {
        case (Some(snapshot), r) =>
          doSideEffect(snapshot.state)
            .map(state => Some(replaceContent(snapshot, state)) -> r)(Threads.PiggyBack)
        case t =>
          Future successful t
      }
    }
  }

  final abstract override def write(key: SID, snapshot: Snapshot): Future[Unit] = {
    doSideEffect(snapshot.state).flatMap { state =>
      super.write(key, replaceContent(snapshot, state))
    }(Threads.PiggyBack)
  }

  final abstract override def writeBatch(batch: Map[SID, Snapshot]): Future[Unit] = {
    val futureWrites = batch.map {
      case (key, snapshot) => write(key, snapshot)
    }
      implicit def ec = Threads.PiggyBack
    Future.sequence(futureWrites).map(_ => ())
  }


}

sealed trait RecursiveUpsert[SID] {
  store: StreamProcessStore[SID, _, _] =>
  /** Retry limit. `0` means no retry (generally not recommneded). */
  protected def upsertRetryLimit: Int = 10
  protected type ConflictingSnapshot = Snapshot
  protected type ContentUpdated = Boolean

  protected def retriesExhausted(id: SID, curr: Snapshot, upd: Snapshot): Nothing = {
    val retryLimit = upsertRetryLimit max 0
    val retryMsg = retryLimit match {
      case 0 => "no retries attempted."
      case 1 => "after 1 retry."
      case _ => s"after $retryLimit retries."
    }
    throw new IllegalStateException(
      s"Failed to update existing snapshot for id $id, from rev:${curr.revision}/tick:${curr.tick} to rev:${upd.revision}/tick:${upd.tick}, $retryMsg")
  }
}

trait NonBlockingRecursiveUpsert[SID, S, U]
extends RecursiveUpsert[SID] {
  store: StreamProcessStore[SID, S, U] =>

  /** Update and return updated snapshot, if any. */
  final protected def upsertRecursive[R](
      id: SID, existing: Option[Snapshot],
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)],
      writeIfExpected: (SID, Option[Snapshot], Snapshot) => Future[Either[ConflictingSnapshot, ContentUpdated]],
      retries: Int)(
      implicit
      updateContext: ExecutionContext,
      updateCodec: UpdateCodec[S, U]): Future[(Option[Update], R)] = {
    updateThunk(existing).flatMap {
      case (result, payload) =>
        // If no result OR exact same content, revision, and tick
        if (result.isEmpty || result == existing) Future.successful(None -> payload)
        else {
          val newSnapshot = result.get // ! isEmpty
          writeIfExpected(id, existing, newSnapshot) flatMap {
            case Right(contentUpdated) =>
              val update = updateCodec.asUpdate(existing, newSnapshot, contentUpdated)
              Future successful Some(update) -> payload
            case Left(conflict) =>
              if (retries > 0) {
                upsertRecursive(id, Some(conflict), updateThunk, writeIfExpected, retries - 1)
              } else retriesExhausted(id, conflict, newSnapshot)
          }
        }
    }
  }

}

trait BlockingRecursiveUpsert[SID, S, U]
extends RecursiveUpsert[SID] {
  store: StreamProcessStore[SID, S, U] =>

  /**
   * Update and return updated snapshot, if any.
   * @note The `upsertThunk` will be run on the
   * provided execution context, but all other
   * code will stay on the thread used to call
   * this method, by blocking and awaiting
   * `updateThunk`.
   */
  @tailrec
  final protected def upsertRecursive[R](
      id: SID, existing: Option[Snapshot],
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)],
      updateThunkTimeout: FiniteDuration,
      writeIfExpected: (SID, Option[Snapshot], Snapshot) => Either[ConflictingSnapshot, ContentUpdated],
      retries: Int)(
      implicit
      updateContext: ExecutionContext,
      updateCodec: UpdateCodec[S, U]): (Option[Update], R) = {

    val updated = Future(updateThunk(existing)).flatMap(identity)
    blocking(updated.await(updateThunkTimeout)) match {
      case (result, payload) =>
        if (result.isEmpty || result == existing) None -> payload
        else {
          val newSnapshot = result.get // ! isEmpty
          writeIfExpected(id, existing, newSnapshot) match {
            case Right(contentUpdated) =>
              val update = updateCodec.asUpdate(existing, newSnapshot, contentUpdated)
              Some(update) -> payload
            case Left(conflict) =>
              if (retries > 0) {
                upsertRecursive(
                  id, Some(conflict),
                  updateThunk, updateThunkTimeout,
                  writeIfExpected, retries - 1)
              } else retriesExhausted(id, conflict, newSnapshot)
          }
        }
    }

  }

}

/**
 * Partial implementation of upsert method,
 * relying on non-blocking atomic `writeIfAbsent` and
 * `writeReplacement` behavior.
 */
trait NonBlockingCASWrites[SID, S, U]
extends NonBlockingRecursiveUpsert[SID, S, U] {
  store: StreamProcessStore[SID, S, U] =>

  /**
   *  Write snapshot, if absent.
   *  Otherwise return present snapshot.
   *  @return `None` if write was successful, or `Some` present snapshot
   */
  protected def writeIfAbsent(key: SID, snapshot: Snapshot): Future[Option[Snapshot]]
  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  protected def writeReplacement(key: SID, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]]

  implicit protected def updateCodec: UpdateCodec[S, U]

  def upsert[R](id: SID)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)] = {

      // Write snapshot, if current expectation holds.
      def writeIfExpected(
          id: SID, expected: Option[Snapshot], snapshot: Snapshot)
          : Future[Either[ConflictingSnapshot, ContentUpdated]] = {
        val contentUpdated = !expected.exists(_ stateEquals snapshot)
        if (contentUpdated) {
          val potentialWriteConflict = expected match {
            case Some(expected) => writeReplacement(id, expected, snapshot)
            case _ => writeIfAbsent(id, snapshot)
          }
          potentialWriteConflict.map(_.toLeft(right = true))
        } else {
          refresh(id, snapshot.revision, snapshot.tick).map(_ => RightFalse)
        }
      }

    read(id).flatMap(upsertRecursive(id, _, updateThunk, writeIfExpected, upsertRetryLimit))
  }

}

/**
 * Partial implementation of upsert method,
 * expecting non-thread-safe blocking connection.
 */
trait BlockingCASWrites[SID, S, U, Conn]
extends BlockingRecursiveUpsert[SID, S, U] {
  store: StreamProcessStore[SID, S, U] =>

  /** Read existing snapshot for updating. */
  protected def readForUpdate[R](id: SID)(thunk: (Conn, Option[Snapshot]) => R): Future[R]
  /** Refresh snapshot. */
  protected def refreshKey(conn: Conn)(id: SID, revision: Revision, tick: Tick): Unit
  /**
   *  Write snapshot, if absent.
   *  Otherwise return present snapshot.
   *  @return `None` if write was successful, or `Some` present snapshot
   */
  protected def writeIfAbsent(conn: Conn)(id: SID, snapshot: Snapshot): Option[Snapshot]
  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  protected def writeReplacement(conn: Conn)(id: SID, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot]

  protected val updateThunkTimeout: FiniteDuration = 11.seconds

  implicit protected def updateCodec: UpdateCodec[S, U]

  def upsert[R](id: SID)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)] = {

      // Write snapshot, if current expectation holds.
      def tryWriteSnapshot(conn: Conn)(
          id: SID, expected: Option[Snapshot], snapshot: Snapshot)
          : Either[ConflictingSnapshot, ContentUpdated] = {
        val contentUpdated = !expected.exists(_ stateEquals snapshot)
        if (contentUpdated) {
          val writeConflict: Option[Snapshot] = expected match {
            case Some(expected) => writeReplacement(conn)(id, expected, snapshot)
            case _ => writeIfAbsent(conn)(id, snapshot)
          }
          writeConflict.toLeft(right = contentUpdated)
        } else {
          refreshKey(conn)(id, snapshot.revision, snapshot.tick)
          RightFalse
        }
      }

    readForUpdate(id) {
      case (conn, existing) =>
        upsertRecursive(id, existing, updateThunk, updateThunkTimeout, tryWriteSnapshot(conn), upsertRetryLimit)
    }

  }

}

private class StreamProcessStoreAdapter[WKey, Work, SKey, Stored, U](
  underlying: StreamProcessStore[SKey, Stored, U],
  keyAdapter: scuff.Codec[WKey, SKey],
  valueAdapter: AsyncCodec[Work, Stored],
  asyncContext: ExecutionContext)
extends StreamProcessStore[WKey, Work, U] {

  def name: String = underlying.name

  import scuff.concurrent.Threads.PiggyBack

  @inline
  private final def w2s(w: WKey): SKey = keyAdapter encode w
  @inline
  private final def w2s(w: Snapshot): Future[delta.Snapshot[Stored]] = {
    valueAdapter.encode(w.state)(asyncContext).map { s =>
      w.copy(state = s)
    }(PiggyBack)
  }
  @inline
  private final def s2w(b: delta.Snapshot[Stored]): Snapshot = b.map(valueAdapter.decode)
  @inline
  private def s2w(b: SKey): WKey = keyAdapter decode b

  def read(key: WKey): Future[Option[Snapshot]] =
    underlying.read(w2s(key)).map(_.map(s2w))(PiggyBack)

  def write(key: WKey, snapshot: Snapshot): Future[Unit] =
    w2s(snapshot).flatMap { snapshot =>
      underlying.write(w2s(key), snapshot)
    }(PiggyBack)

  def readBatch(keys: Iterable[WKey]): Future[Map[WKey, Snapshot]] =
    underlying.readBatch(keys.map(w2s)).map {
      _.map {
        case (key, snapshot) =>
          s2w(key) -> s2w(snapshot)
      }
    }(PiggyBack)

  def refresh(key: WKey, revision: Revision, tick: Tick): Future[Unit] =
    underlying.refresh(this w2s key, revision, tick)

  def refreshBatch(revisions: Map[WKey, (Int, Long)]): Future[Unit] =
    underlying refreshBatch revisions.map {
      case (key, revTick) => (this w2s key) -> revTick
    }

  def writeBatch(batch: Map[WKey, Snapshot]): Future[Unit] = {
      implicit def ec = PiggyBack
    val batchS = batch.iterator.map {
      case (key, snapshotW) =>
        w2s(snapshotW)
          .map(w2s(key) -> _)
    }
    Future.sequence(batchS)
      .flatMap { batchS =>
        underlying writeBatch batchS.toMap
      }
  }

  def tickWatermark: Option[Tick] = underlying.tickWatermark

  def upsert[R](key: WKey)(updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)] = {

    underlying.upsert[R](this w2s key) { oldS =>
      val oldW: Option[Snapshot] = oldS.map(s2w)
      updateThunk(oldW).flatMap {
        case (Some(newW), r) =>
          w2s(newW).map { newS =>
            Some(newS) -> r
          }(PiggyBack)
        case (_, r) =>
          Future successful None -> r
      }
    }

  }

}

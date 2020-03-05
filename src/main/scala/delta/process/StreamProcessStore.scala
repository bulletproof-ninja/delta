package delta.process

import collection.Map
import delta.SnapshotStore
import scala.concurrent._
import scala.concurrent.duration._
import scuff.concurrent._
import scala.Right
import scuff._
import scala.annotation.tailrec

/**
 * A `StreamProcessStore` keeps track of how far a stream
 * has been processed, and stores any necessary state derived
 * from that stream.
 * @tparam K The key/id type
 * @tparam S The state type. NOTE: Consider using a type that
 * is optimized for reading, if this store will serve as a read
 * model, and then `adapt` it for the actual processing setup.
 * @tparam U The update type. For small datasets, this is often
 * just the same as `S`. But for larger datasets, it can be a
 * diff'ed format.
 */
trait StreamProcessStore[K, S, U] extends SnapshotStore[K, S] {

  type Update = delta.process.Update[U]

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]]
  def writeBatch(batch: Map[K, Snapshot]): Future[Unit]
  def refresh(key: K, revision: Int, tick: Long): Future[Unit]
  def refreshBatch(revisions: Map[K, (Int, Long)]): Future[Unit]

  /**
   *  Tick watermark. This is a best-effort
   *  watermark, usually either the highest
   *  or last processed. Combined with a
   *  potential tick skew, this is used to
   *  determine where to resume any interrupted
   *  stream processing.
   */
  def tickWatermark: Option[Long]

  /**
   * Update/Insert.
   * @param key The key to update
   * @param updateThunk The update function. Return `None` if no insert/update is desired
   * @param updateContext Execution context for running the update thunk
   * @return The result of the `updateThunk` function
   */
  def upsert[R](key: K)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)]

  def adaptState[W](stateCodec: Codec[W, S]): StreamProcessStore[K, W, U] =
    adapt(Codec.noop, AsyncCodec(stateCodec), Threads.PiggyBack)

  def adaptState[W](stateCodec: AsyncCodec[W, S], asyncContext: ExecutionContext)
      : StreamProcessStore[K, W, U] =
    adapt(Codec.noop, stateCodec, asyncContext)

  def adaptKeys[W](keyCodec: Codec[W, K])
      : StreamProcessStore[W, S, U] =
    adapt(keyCodec, AsyncCodec.noop, Threads.PiggyBack)


  def adapt[WK, W](
      keyCodec: Codec[WK, K],
      stateCodec: AsyncCodec[W, S],
      asyncContext: ExecutionContext): StreamProcessStore[WK, W, U] =
    new StreamProcessStoreAdapter[WK, W, K, S, U](this, keyCodec, stateCodec, asyncContext)

}

/**
 * Trait supporting side-effecting stream processing.
 * NOTE: Side-effects are always at-least-once semantics.
 */
trait SideEffect[K, S <: AnyRef, U]
extends StreamProcessStore[K, S, U] {

  /** Perform side-effect, if necessary, and return final state. */
  protected def doSideEffect(state: S): Future[S]

  @inline
  private def replaceContent(snapshot: Snapshot, newContent: S): Snapshot =
    if (snapshot.content eq newContent) snapshot
    else snapshot.copy(content = newContent)

  final abstract override def upsert[R](key: K)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)] = {
    super.upsert(key) { optSnapshot =>
      updateThunk(optSnapshot).flatMap {
        case (Some(snapshot), r) =>
          doSideEffect(snapshot.content)
            .map(state => Some(replaceContent(snapshot, state)) -> r)(Threads.PiggyBack)
        case t =>
          Future successful t
      }
    }
  }

  final abstract override def write(key: K, snapshot: Snapshot): Future[Unit] = {
    doSideEffect(snapshot.content).flatMap { state =>
      super.write(key, replaceContent(snapshot, state))
    }(Threads.PiggyBack)
  }

  final abstract override def writeBatch(batch: Map[K, Snapshot]): Future[Unit] = {
    val futureWrites = batch.map {
      case (key, snapshot) => write(key, snapshot)
    }
      implicit def ec = Threads.PiggyBack
    Future.sequence(futureWrites).map(_ => ())
  }


}

sealed trait RecursiveUpsert[K, S, U] {
  store: StreamProcessStore[K, S, U] =>
  /** Retry limit. `0` means no retry (generally not recommneded). */
  protected def upsertRetryLimit: Int = 10
  protected type ConflictingSnapshot = Snapshot
  protected type ContentUpdated = Boolean

  protected def retriesExhausted(key: K, curr: Snapshot, upd: Snapshot): Nothing = {
    val retryLimit = upsertRetryLimit max 0
    val retryMsg = retryLimit match {
      case 0 => "no retries attempted."
      case 1 => "after 1 retry."
      case _ => s"after $retryLimit retries."
    }
    throw new IllegalStateException(
      s"Failed to update existing snapshot for key $key, from rev:${curr.revision}/tick:${curr.tick} to rev:${upd.revision}/tick:${upd.tick}, $retryMsg")
  }
}

trait NonBlockingRecursiveUpsert[K, S, U]
extends RecursiveUpsert[K, S, U] {
  store: StreamProcessStore[K, S, U] =>

  /** Update and return updated snapshot, if any. */
  final protected def upsertRecursive[R](
      key: K, existing: Option[Snapshot],
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)],
      writeIfExpected: (K, Option[Snapshot], Snapshot) => Future[Either[ConflictingSnapshot, ContentUpdated]],
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
          writeIfExpected(key, existing, newSnapshot) flatMap {
            case Right(contentUpdated) =>
              val update = updateCodec.asUpdate(existing, newSnapshot, contentUpdated)
              Future successful Some(update) -> payload
            case Left(conflict) =>
              if (retries > 0) {
                upsertRecursive(key, Some(conflict), updateThunk, writeIfExpected, retries - 1)
              } else retriesExhausted(key, conflict, newSnapshot)
          }
        }
    }
  }

}

trait BlockingRecursiveUpsert[K, S, U]
  extends RecursiveUpsert[K, S, U] {
  store: StreamProcessStore[K, S, U] =>

  protected def blockingCtx: ExecutionContext

  /**
   * Update and return updated snapshot, if any.
   * NOTE: The `upsertThunk` will be run on the
   * provided execution context, but all other
   * code will stay on the thread used to call
   * this method, by blocking and awaiting
   * `updateThunk`.
   */
  @tailrec
  final protected def upsertRecursive[R](
      key: K, existing: Option[Snapshot],
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)],
      updateThunkTimeout: FiniteDuration,
      writeIfExpected: (K, Option[Snapshot], Snapshot) => Either[ConflictingSnapshot, ContentUpdated],
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
          writeIfExpected(key, existing, newSnapshot) match {
            case Right(contentUpdated) =>
              val update = updateCodec.asUpdate(existing, newSnapshot, contentUpdated)
              Some(update) -> payload
            case Left(conflict) =>
              if (retries > 0) {
                upsertRecursive(
                  key, Some(conflict),
                  updateThunk, updateThunkTimeout,
                  writeIfExpected, retries - 1)
              } else retriesExhausted(key, conflict, newSnapshot)
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
trait NonBlockingCASWrites[K, S, U]
extends NonBlockingRecursiveUpsert[K, S, U] {
  store: StreamProcessStore[K, S, U] =>

  /**
   *  Write snapshot, if absent.
   *  Otherwise return present snapshot.
   *  @return `None` if write was successful, or `Some` present snapshot
   */
  protected def writeIfAbsent(key: K, snapshot: Snapshot): Future[Option[Snapshot]]
  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  protected def writeReplacement(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]]

  implicit protected def updateCodec: UpdateCodec[S, U]

  def upsert[R](key: K)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)] = {

      // Write snapshot, if current expectation holds.
      def writeIfExpected(
          key: K, expected: Option[Snapshot], snapshot: Snapshot)
          : Future[Either[ConflictingSnapshot, ContentUpdated]] = {
        val contentUpdated = !expected.exists(_.contentEquals(snapshot))
        if (contentUpdated) {
          val potentialWriteConflict = expected match {
            case Some(expected) => writeReplacement(key, expected, snapshot)
            case _ => writeIfAbsent(key, snapshot)
          }
          potentialWriteConflict.map(_.toLeft(right = true))
        } else {
          refresh(key, snapshot.revision, snapshot.tick).map(_ => Right(false))
        }
      }

    read(key).flatMap(upsertRecursive(key, _, updateThunk, writeIfExpected, upsertRetryLimit))
  }

}

/**
 * Partial implementation of upsert method,
 * expecting non-thread-safe blocking connection.
 */
trait BlockingCASWrites[K, S, U, Conn]
extends BlockingRecursiveUpsert[K, S, U] {
  store: StreamProcessStore[K, S, U] =>

  /** Read existing snapshot for updating. */
  protected def readForUpdate[R](key: K)(thunk: (Conn, Option[Snapshot]) => R): R
  /** Refresh snapshot. */
  protected def refreshKey(conn: Conn)(key: K, revision: Int, tick: Long): Unit
  /**
   *  Write snapshot, if absent.
   *  Otherwise return present snapshot.
   *  @return `None` if write was successful, or `Some` present snapshot
   */
  protected def writeIfAbsent(conn: Conn)(key: K, snapshot: Snapshot): Option[Snapshot]
  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  protected def writeReplacement(conn: Conn)(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot]

  protected val updateThunkTimeout: FiniteDuration = 11.seconds

  implicit protected def updateCodec: UpdateCodec[S, U]

  def upsert[R](key: K)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)] = {

      // Write snapshot, if current expectation holds.
      def tryWriteSnapshot(conn: Conn)(
          key: K, expected: Option[Snapshot], snapshot: Snapshot)
          : Either[ConflictingSnapshot, ContentUpdated] = {
        val contentUpdated = !expected.exists(_ contentEquals snapshot)
        if (contentUpdated) {
          val writeConflict: Option[Snapshot] = expected match {
            case Some(expected) => writeReplacement(conn)(key, expected, snapshot)
            case _ => writeIfAbsent(conn)(key, snapshot)
          }
          writeConflict.toLeft(right = contentUpdated)
        } else {
          refreshKey(conn)(key, snapshot.revision, snapshot.tick)
          Right(false)
        }
      }

    Future {
      readForUpdate(key) {
        case (conn, existing) =>
          upsertRecursive(key, existing, updateThunk, updateThunkTimeout, tryWriteSnapshot(conn), upsertRetryLimit)
      }
    }(blockingCtx) // <- MUST run on the blocking context, otherwise will dead-lock
  }

}

class StreamProcessStoreAdapter[WKey, Work, SKey, Stored, U](
  underlying: StreamProcessStore[SKey, Stored, U],
  keyAdapter: scuff.Codec[WKey, SKey],
  valueAdapter: AsyncCodec[Work, Stored],
  asyncContext: ExecutionContext)
extends StreamProcessStore[WKey, Work, U] {

  import scuff.concurrent.Threads.PiggyBack

  @inline
  private final def w2s(w: WKey): SKey = keyAdapter encode w
  @inline
  private final def w2s(w: Snapshot): Future[delta.Snapshot[Stored]] = {
    valueAdapter.encode(w.content)(asyncContext).map { s =>
      w.copy(content = s)
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

  def refresh(key: WKey, revision: Int, tick: Long): Future[Unit] =
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

  def tickWatermark: Option[Long] = underlying.tickWatermark

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

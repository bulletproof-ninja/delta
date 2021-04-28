package delta.process

import delta.SnapshotStore

import concurrent._
import concurrent.duration._
import scala.annotation.tailrec

import scuff._
import scuff.concurrent._
import scala.util.Failure
import scala.util.Success

/**
 * A `StreamProcessStore` keeps track of how far a stream
 * has been processed, and stores any necessary state derived
 * from that stream.
 *
 * @note For state type `S`, consider using a type that is
 * optimized for reading (e.g. a plain JSON `String`) as this makes
 * it easily available as a [[delta.read.ReadModel]] thus just
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
  type Channel = delta.Channel

  def name: String
  def channels: Set[Channel]

  override def toString() = s"${getClass.getName}($name)"

  type Update = delta.process.Update[U]
  type WriteErrors = Map[SID, Throwable]

  protected final val NoWriteErrors = Map.empty[SID, Throwable]
  protected final val NoWriteErrorsFuture = Future successful NoWriteErrors

  def readBatch(keys: IterableOnce[SID]): Future[collection.Map[SID, Snapshot]]

  /**
    * Write batch of snapshots.
    * @note The returned future might contain a batch write error itself,
    * or individual write errors, depending on underlying implementation.
    * @param batch Map of snapshots to write
    * @return Map of individual write errors, if any
    */
  def writeBatch(batch: IterableOnce[(SID, Snapshot)]): Future[WriteErrors]
  def refresh(key: SID, revision: Revision, tick: Tick): Future[Unit]
  /**
    * Write batch of revision/tick updates.
    * @note The returned future might contain a batch update error itself,
    * or individual update errors, depending on underlying implementation.
    * @param revisions Map of revisions to write
    * @return Map of individual update errors, if any
    */
  def refreshBatch(revisions: IterableOnce[(SID, Int, Long)]): Future[WriteErrors]

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
  def adaptState[W](
      stateCodec: Codec[W, S])
      : StreamProcessStore[SID, W, U] = {
    val asAsync = AsyncCodec(stateCodec)
    adapt(Codec.noop, _ => asAsync)
  }

  /** Adapt `S` state type for processing. */
  def adaptState[W](
      stateCodec: SID => AsyncCodec[W, S])
      : StreamProcessStore[SID, W, U] =
    adapt(Codec.noop, stateCodec)

  /** Adapt `SID` key/id type for processing. */
  def adaptKeys[W](
      keyCodec: Codec[W, SID])
      : StreamProcessStore[W, S, U] =
    adapt(keyCodec, _ => AsyncCodec.noop)

  /** Adapt `SID` key/id and `S` state types for processing. */
  def adapt[WK, W](
      keyCodec: Codec[WK, SID],
      stateCodec: SID => AsyncCodec[W, S])
      : StreamProcessStore[WK, W, U] =
    new StreamProcessStoreAdapter[WK, W, SID, S, U](this, keyCodec, stateCodec)

}

/**
 * Trait supporting side-effecting stream processing.
 *
 * @note Side-effects are always at-least-once semantics
 * (side effects must complete successfully before transaction processing is considered processed),
 * so ensure idempotent behavior if possible.
 */
trait SideEffects[SID, S <: AnyRef, U]
extends StreamProcessStore[SID, S, U] {

  /** Apply side-effect(s), if necessary, and return final state. */
  protected def applySideEffects(id: SID, state: S): Future[S]

  @inline
  private def replaceContent(snapshot: Snapshot, newState: S): Snapshot =
    if (snapshot.state eq newState) snapshot
    else snapshot.copy(state = newState)

  final abstract override def upsert[R](key: SID)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext)
      : Future[(Option[Update], R)] = {
    super.upsert(key) { optSnapshot =>
      updateThunk(optSnapshot).flatMap {
        case (Some(snapshot), r) =>
          applySideEffects(key, snapshot.state)
            .map(state => Some(replaceContent(snapshot, state)) -> r)(Threads.PiggyBack)
        case t =>
          Future successful t
      }
    }
  }

  final abstract override def write(key: SID, snapshot: Snapshot): Future[Unit] = {
    applySideEffects(key, snapshot.state).flatMap { state =>
      super.write(key, replaceContent(snapshot, state))
    }(Threads.PiggyBack)
  }

  final abstract override def writeBatch(
      batch: IterableOnce[(SID, Snapshot)])
      : Future[WriteErrors] = {

    implicit def ec = Threads.PiggyBack

    val futureWrites = batch.iterator.map {
      case (key, snapshot) =>
        key -> write(key, snapshot)
    }

    Future.sequenceTry(futureWrites)(_._2, _._1)
      .map {
        _.collect {
          case (Failure(failure), key) => key -> failure
        }
        .foldLeft(Map.empty[SID, Throwable]) { _ + _ }

    }

  }


}

sealed trait RecursiveUpsert[SID] {
  store: StreamProcessStore[SID, _, _] =>
  /** Retry limit. `0` means no retry (generally not recommneded). */
  protected def upsertRetryLimit: Int = 9
  protected type ConflictingSnapshot = Snapshot
  protected type ContentUpdated = Boolean

  protected def retriesExhausted(id: SID, existing: Option[Snapshot], actual: Snapshot, upd: Snapshot): Nothing = {
    val attempts = (upsertRetryLimit max 0) + 1
    val attemptMsg = attempts match {
      case 1 => "after 1 attempt (no retries)"
      case _ => s"after $attempts attempts"
    }
    val expected = existing.map(s => s"existing snapshot from rev:${s.revision}/tick:${s.tick}") getOrElse "(new)"
    throw new IllegalStateException(
      s"Failed to update `$id` $expected to rev:${upd.revision}/tick:${upd.tick}, $attemptMsg. Current: rev:${actual.revision}/tick:${actual.tick}")
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
              } else retriesExhausted(id, existing, conflict, newSnapshot)
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
      updateThunkAwaitTimeout: FiniteDuration,
      writeIfExpected: (SID, Option[Snapshot], Snapshot) => Either[ConflictingSnapshot, ContentUpdated],
      retries: Int)(
      implicit
      updateCodec: UpdateCodec[S, U]): (Option[Update], R) = {

   val futureUpdated = updateThunk(existing)
    val (updated, payload) = // Blocking wait, to stay on the same thread for `writeIfExpected`
      try futureUpdated.await(updateThunkAwaitTimeout) catch {
        case _: TimeoutException =>
          throw new TimeoutException(
            s"Upsert of `$id` timed out after $updateThunkAwaitTimeout, while executing: $updateThunk")
      }
    if (updated.isEmpty || updated == existing) None -> payload
    else {
      val newSnapshot = updated.get // ! isEmpty
      writeIfExpected(id, existing, newSnapshot) match {
        case Right(contentUpdated) =>
          val update = updateCodec.asUpdate(existing, newSnapshot, contentUpdated)
          Some(update) -> payload
        case Left(conflict) =>
          if (retries > 0) {
            upsertRecursive(
              id, Some(conflict),
              updateThunk, updateThunkAwaitTimeout,
              writeIfExpected, retries - 1)
          } else retriesExhausted(id, existing, conflict, newSnapshot)
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

  private[this] val defaultBlockingTimeout = 30.seconds
  protected def updateThunkBlockingTimeout = defaultBlockingTimeout // probably excessive, but if not, override

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
        upsertRecursive(id, existing, updateThunk, updateThunkBlockingTimeout, tryWriteSnapshot(conn), upsertRetryLimit)
    }

  }

}

private class StreamProcessStoreAdapter[WKey, InUse, SKey, AtRest, U](
  underlying: StreamProcessStore[SKey, AtRest, U],
  keyAdapter: scuff.Codec[WKey, SKey],
  valueAdapter: SKey => AsyncCodec[InUse, AtRest])
extends StreamProcessStore[WKey, InUse, U] {

  def name: String = underlying.name
  def channels: Set[Channel] = underlying.channels

  import scuff.concurrent.Threads.PiggyBack

  @inline
  private final def w2s(w: WKey): SKey = keyAdapter encode w
  @inline
  private final def w2s(k: SKey, w: Snapshot): Future[delta.Snapshot[AtRest]] = {
    valueAdapter(k)
      .encode(w.state)
      .map { s =>
        w.copy(state = s)
      }(PiggyBack)
  }
  @inline
  private final def s2w(k: SKey, b: delta.Snapshot[AtRest]): Snapshot = b.map(valueAdapter(k).decode)
  @inline
  private def s2w(b: SKey): WKey = keyAdapter decode b

  def read(key: WKey): Future[Option[Snapshot]] = {
    val sKey = w2s(key)
    underlying.read(sKey).map(_.map(s2w(sKey, _)))(PiggyBack)
  }

  def write(key: WKey, snapshot: Snapshot): Future[Unit] = {
    val sKey = w2s(key)
    w2s(sKey, snapshot).flatMap { snapshot =>
      underlying.write(sKey, snapshot)
    }(PiggyBack)
  }

  def readBatch(keys: IterableOnce[WKey]): Future[collection.Map[WKey, Snapshot]] =
    underlying.readBatch(keys.iterator.map(w2s)).map {
      _.map {
        case (key, snapshot) =>
          s2w(key) -> s2w(key, snapshot)
      }
    }(PiggyBack)

  def refresh(key: WKey, revision: Revision, tick: Tick): Future[Unit] =
    underlying.refresh(this w2s key, revision, tick)

  def refreshBatch(
      revisions: IterableOnce[(WKey, Int, Long)])
      : Future[WriteErrors] = {

    val refreshResult = underlying refreshBatch revisions.iterator.map {
      case (key, rev, tick) => (this w2s key, rev, tick)
    }
    refreshResult.map {
      _.map {
        case (key, failure) => (this s2w key) -> failure
      }
    }(Threads.PiggyBack)

  }

  def writeBatch(
      batch: IterableOnce[(WKey, Snapshot)])
      : Future[WriteErrors] = {

    implicit def ec = PiggyBack

    val convertedBatch = batch.iterator.map {
      case (wKey, snapshotW) =>
        val sKey = w2s(wKey)
        sKey ->
          w2s(sKey, snapshotW)
    }

    Future.sequenceTry(convertedBatch)(_._2, _._1)
      .flatMap { batch =>
        val (snapshots, conversionErrors) =
          batch.partition(_._1.isSuccess) match {
            case (trySuccesses, tryFailures) =>
              val successes = trySuccesses.collect { case (Success(snapshot), sKey) => sKey -> snapshot }
              val failures = tryFailures.collect { case (Failure(error), sKey) => s2w(sKey) -> error }
              successes -> failures
          }

        val writeResult = underlying writeBatch snapshots.to(Iterable)
        writeResult.map { writeErrors =>
          val adaptedErrors = writeErrors.map {
            case (sKey, error) => s2w(sKey) -> error
          }
          conversionErrors.foldLeft(adaptedErrors) {
            case (errors, error) => errors + error
          }
        }
      }

  }

  def tickWatermark: Option[Tick] = underlying.tickWatermark

  def upsert[R](key: WKey)(updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[Update], R)] = {

    val sKey = w2s(key)
    underlying.upsert[R](sKey) { oldS =>
      val oldW: Option[Snapshot] = oldS.map(s2w(sKey, _))
      updateThunk(oldW).flatMap {
        case (Some(newW), r) =>
          w2s(sKey, newW).map { newS =>
            Some(newS) -> r
          }(PiggyBack)
        case (_, r) =>
          Future successful None -> r
      }
    }

  }

}

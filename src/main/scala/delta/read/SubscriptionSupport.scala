package delta.read

import delta._

import scala.util.{ Success, Failure }
import scala.concurrent._, duration.FiniteDuration

import scuff.concurrent._
import scuff.Subscription

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicBoolean

trait SubscriptionSupport[ID, S, U]
extends StreamId {
  rm: ReadModel[ID, S] =>

  type Update = delta.process.Update[U]

  /**
    * Update previous state to current state, if possible.
    * This is an optional method that can improve performance
    * in some cases (it's optional in the sense that it can simply
    * return `None` without affecting functionality).
    * @note If `S` and `U` are the same actual or conceptual types (common),
    * then this is trivial to implement, as they are interchangeable.
    * @return Updated state, or `None` if not possible
    */
  protected def updateState(id: ID, prevState: Option[S], update: U): Option[S]
  /** The default timeout for reads, if not provided. */
  protected def defaultReadTimeout: FiniteDuration
  /** Scheduler used for `read` timeouts and replay delay scheduling. */
  protected def scheduler: ScheduledExecutorService
  /** Update subscription stub. */
  protected def subscribe(id: ID)(callback: Update => Unit): Subscription

  def read(id: ID, minTick: Tick)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    read(id, minTick, defaultReadTimeout)

  def read(id: ID, minRevision: Revision)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    read(id, minRevision, defaultReadTimeout)

  /**
   * Read snapshot, ensuring it's at least the given revision,
   * waiting the supplied timeout if current revision is stale.
   * @param id The read identifier
   * @param minRevision The minimum revision to return
   * @param timeout Read timeout. This timeout is only used if `minRevision` doesn't match
   * @return Snapshot >= `minRevision` or [[delta.read.ReadRequestFailure]] cannot fullfil
   */
  def read(id: ID, minRevision: Revision, timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readMin(id, timeout, minRevision = minRevision)

  /**
   * Read snapshot, ensuring it's at least the given revision,
   * waiting the supplied timeout if current revision is stale.
   * @param id The read identifier
   * @param minRevision The minimum revision to return
   * @param timeout Optional alternate timeout.
   * @return Snapshot >= `minRevision` or [[delta.read.ReadRequestFailure]] cannot fullfil
   */
  def read(id: ID, minRevision: Revision, timeout: Option[FiniteDuration])(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readMin(id, timeout getOrElse defaultReadTimeout, minRevision = minRevision)

  /**
   * Read snapshot, ensuring it's at least the given tick,
   * waiting the supplied timeout if current is too old.
   * @param id The read identifier
   * @param afterTick The tick value the snapshot must succeed
   * @param timeout Lookup timeout. This timeout is only used if `afterTick` doesn't match
   * @return Snapshot >= `minTick` or [[delta.read.ReadRequestFailure]] cannot fullfil
   */
  def read(id: ID, minTick: Tick, timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readMin(id, timeout, minTick = minTick)

  /**
   * Read snapshot, ensuring it's at least the given tick,
   * waiting the supplied or default timeout if current is too old.
   * @param id The read identifier
   * @param afterTick The tick value the snapshot must succeed
   * @param timeout Optional alternate timeout.
   * @return Snapshot >= `minTick` or [[delta.read.ReadRequestFailure]] cannot fullfil
   */
  def read(id: ID, minTick: Tick, timeout: Option[FiniteDuration])(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readMin(id, timeout getOrElse defaultReadTimeout, minTick = minTick)

  private abstract class Callback(id: ID)
  extends Runnable {
    override def hashCode = id.##
  }

  private final class SnapshotCallback(
    id: ID, snapshot: Option[Snapshot], callback: Either[Snapshot, Update] => Unit,
    snapshotSeen: AtomicBoolean,
    pendingUpdatesRef: AtomicReference[List[Update]])
  extends Callback(id) {

    def run(): Unit = {
      val revision =
        snapshot.map { snapshot =>
          callback(Left(snapshot))
          snapshotSeen set true
          snapshot.revision
        } getOrElse -1
      val pendingUpdates = pendingUpdatesRef.getAndSet(null).sortBy(_.revision)
      pendingUpdates.iterator
        .dropWhile(_.revision <= revision)
        .map(Right(_))
        .foreach(callback)
    }

  }

  private final class UpdateCallback(
    id: ID, update: Update, callback: Either[Snapshot, Update] => Unit,
    snapshotSeen: AtomicBoolean,
    pendingUpdatesRef: AtomicReference[List[Update]])
  extends Callback(id) {

    def run(): Unit = {
      pendingUpdatesRef.get() match {

        case null =>
          val snapshot =
            if (update.revision == 0 && !snapshotSeen.get)
              update
                .flatMap(updateState(id, None, _))
                .toSnapshot
            else None

          if (snapshot.isEmpty)
            callback(Right(update))
          else
            callback(Left(snapshot.get))

        case pendingUpdates =>
          if ( ! pendingUpdatesRef.compareAndSet(pendingUpdates, update :: pendingUpdates) ) {
            run()
          }

      }
    }

  }

  /**
   * Read & subscribe. Consistent reads, with initial snapshot, which eliminates
   * any potential race conditions otherwise possible if naively doing subscribe/read or read/subscribe.
   *
   * @note If called before transaction revision 0 (genesis transaction), the callback may not receive
   * a `Left` callback, but instead get a `Right` update with revision 0.
   *
   * @param id The subscription identifier (same as read identifier)
   * @param callbackCtx The execution context used for callback. Use single-thread to ensure ordering, if required
   * @param callback The callback function, either `Left` for initial snapshot, or `Right` for subsequent updates.
   * @return Subscription. The future resolves once the initial lookup has been completed.
   */
  def readContinuously(id: ID, callbackCtx: ExecutionContext)(
      callback: Either[Snapshot, Update] => Unit): Future[Subscription] = {

    val pendingUpdates: AtomicReference[List[Update]] = new AtomicReference(Nil) // becomes `null` when read is resolved
    val snapshotSeen = new AtomicBoolean(false)

    val subscription = this.subscribe(id) { update =>
      callbackCtx execute new UpdateCallback(id, update, callback, snapshotSeen, pendingUpdates)
    }
    // Piggy-back on reader thread. Don't use potentially single threaded callbackCtx, which could possibly deadlock.
      implicit def ec = Threads.PiggyBack
    readSnapshot(id)
      .map { snapshot =>
        callbackCtx execute new SnapshotCallback(id, snapshot, callback, snapshotSeen, pendingUpdates)
        subscription
      }
      .andThen {
        case Failure(_) => subscription.cancel()
      }

  }

  /**
    * Second read, to ensure closing race condition gap.
    * @note Defaults to reading snapshot. Override if better
    * implementation is available.
    * @param id identifier
    * @param minRevision The minimum revision expected. Provided as a hint, can be ignored.
    * @param minTick The minimum tick expected. Provided as a hint, can be ignored.
    */
  protected def readAgain(id: ID, minRevision: Revision, minTick: Tick)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] =
    readSnapshot(id)

  private def readMin(
      id: ID, timeout: FiniteDuration, minRevision: Revision = -1, minTick: Tick = Long.MinValue)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = {

      def expected(revision: Revision, tick: Tick): Boolean = {
        if (revision < 0 && minRevision >= 0) {
          throw new IllegalArgumentException(s"Snapshot revision is not supported (possibly because of joined streams). Use `tick` instead.")
        }
        revision >= minRevision && tick >= minTick
      }

      def readAgain(promise: Promise[Snapshot]): Unit = {

        this.readAgain(id, minRevision, minTick) andThen {
          case Success(Some(snapshot)) if expected(snapshot.revision, snapshot.tick) =>
            promise trySuccess snapshot
          case Failure(th) =>
            promise tryFailure th
        }

      }

    readSnapshot(id) flatMap {
      case Some(snapshot) if expected(snapshot.revision, snapshot.tick) =>
        Future successful snapshot
      case maybeSnapshot => // Earlier revision or tick than expected, or unknown id
        if (timeout.length == 0) {
          Future failed Timeout(id, maybeSnapshot, minRevision, minTick, timeout)
        } else {
          val promise = Promise[Snapshot]()
          val subscription = this.subscribe(id) { update =>
            if (expected(update.revision, update.tick)) {
              val snapshotRev = maybeSnapshot.map(_.revision) getOrElse -1
              val updSnapshot = {
                if (snapshotRev != update.revision - 1) None // Cannot reliably update, if not monotonic
                else update.changed match {
                  case Some(updated) =>
                    updateState(id, maybeSnapshot.map(_.state), updated)
                      .map(s => new Snapshot(s, update.revision, update.tick))
                  case _ => // No content change
                    maybeSnapshot
                      .map(_.copy(revision = update.revision, tick = update.tick))
                }
              }
              updSnapshot match {
                case Some(snapshot) =>
                  promise trySuccess snapshot
                case None =>
                  readAgain(promise)
              }
            }
          }
          promise.future onComplete { _ => subscription.cancel() }
          scheduler.schedule(timeout) {
            if (!promise.isCompleted) {
              promise tryFailure Timeout(id, maybeSnapshot, minRevision, minTick, timeout)
            }
          }
          // Unfortunately we have to try another read, to eliminate the race condition
          readAgain(promise)
          promise.future
        }
    }
  }

  /**
    * Read custom value.
    *
    * @param id
    * @param readTimeout Optional timeout, defaults to read model's timeout
    * @param filter Partial filter/transform function. First match is returned
    * @param callbackCtx The execution context to run on
    * @return Custom value as defined by `filter`
    */
  def readCustom[V](id: ID, readTimeout: FiniteDuration = null)(
      filter: PartialFunction[Either[Snapshot, Update], V])
      (implicit callbackCtx: ExecutionContext): Future[V] = {

    val p = Promise[V]()

    val subscription =
      readContinuously(id, callbackCtx) { either =>
        if (filter isDefinedAt either) p trySuccess filter(either)
      }

    val timeout = if (readTimeout != null) readTimeout else defaultReadTimeout
    val timeoutTask = scheduler.schedule(timeout) {
      p tryFailure
        new Timeout(id, timeout, s"Custom read for $id timed out after $timeout")
    }

    p.future.andThen {
      case _ =>
        timeoutTask cancel false
        subscription foreach { _.cancel() }
    }

  }

}

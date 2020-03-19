package delta.read

import scala.util.{ Success, Failure }
import scala.concurrent._, duration.FiniteDuration
import scuff.concurrent._
import scuff.Subscription
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference

trait SubscriptionSupport[ID, S, U]
extends StreamId[ID] {
  rm: BasicReadModel[ID, S] =>

  type Update = delta.process.Update[U]

  /**
    * Update previous state to current state, if possible.
    * This is an optional method that can improve performance
    * in some cases.
    * NOTE: If `S` and `U` are the same conceptual types,
    * then this is trivial to implement, as they are interchangeable.
    * @return Updated state, or `None` if not possible
    */
  protected def updateState(id: ID, prevState: Option[S], update: U): Option[S]

  protected def defaultReadTimeout: FiniteDuration
  /** Scheduler used for `read` timeouts and replay delay scheduling. */
  protected def scheduler: ScheduledExecutorService
  /** Update subscription stub. */
  protected def subscribe(id: ID)(callback: Update => Unit): Subscription

  def read(id: ID, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    read(id, minTick, defaultReadTimeout)

  def read(id: ID, minRevision: Int)(
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
  def read(id: ID, minRevision: Int, timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readMin(id, timeout, minRevision = minRevision)

  /**
   * Read snapshot, ensuring it's at least the given tick,
   * waiting the supplied timeout if current is too old.
   * @param id The read identifier
   * @param afterTick The tick value the snapshot must succeed
   * @param timeout Lookup timeout. This timeout is only used if `afterTick` doesn't match
   * @return Snapshot >= `minTick` or [[delta.read.ReadRequestFailure]] cannot fullfil
   */
  def read(id: ID, minTick: Long, timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readMin(id, timeout, minTick = minTick)

  private abstract class Callback(id: ID)
  extends Runnable {
    override def hashCode = id.##
  }

  private final class SnapshotCallback(
    id: ID, snapshot: Option[Snapshot], callback: Either[Snapshot, Update] => Unit,
    pendingUpdatesRef: AtomicReference[List[Update]])
  extends Callback(id) {

    def run(): Unit = {
      val revision = snapshot.map { snapshot => callback(Left(snapshot)); snapshot.revision } getOrElse -1
      val pendingUpdates = pendingUpdatesRef.getAndSet(null).sortBy(_.revision)
      pendingUpdates.iterator
        .dropWhile(_.revision <= revision)
        .map(Right(_))
        .foreach(callback)
    }

  }

  private final class UpdateCallback(
    id: ID, update: Update, callback: Either[Snapshot, Update] => Unit,
    pendingUpdatesRef: AtomicReference[List[Update]])
  extends Callback(id) {

    def run(): Unit = {
      pendingUpdatesRef.get() match {
        case null =>
          callback(Right(update))
        case pendingUpdates =>
          if ( ! pendingUpdatesRef.compareAndSet(pendingUpdates, update :: pendingUpdates) ) {
            run()
          }
      }
    }

  }

  /**
   * Subscribe, with initial snapshot read. The built-in initial read eliminates
   * any potential race conditions otherwise possible if naively doing subscribe/read or read/subscribe.
   *
   * NOTE: If called before transaction revision 0 (genesis transaction), the callback may not receive
   * a `Left` callback, but instead get a `Right` update with revision 0.
   *
   * @param id The subscription identifier (same as read identifier)
   * @param callbackCtx The execution context used for callback. Use single-thread to ensure ordering, if required
   * @param callback The callback function, either `Left` for initial snapshot, or `Right` for subsequent updates.
   * @return Subscription. The future resolves once the initial lookup has been completed.
   */
  def subscribe(id: ID, callbackCtx: ExecutionContext)(
      callback: Either[Snapshot, Update] => Unit): Future[Subscription] = {

    val pendingUpdates: AtomicReference[List[Update]] = new AtomicReference(Nil) // becomes `null` when read is resolved

    val subscription = this.subscribe(id) { update =>
      callbackCtx execute new UpdateCallback(id, update, callback, pendingUpdates)
    }
    // Piggy-back on reader thread. Don't use potentially single threaded callbackCtx, which could possibly deadlock.
      implicit def ec = Threads.PiggyBack
    readSnapshot(id)
      .map { snapshot =>
        callbackCtx execute new SnapshotCallback(id, snapshot, callback, pendingUpdates)
        subscription
      }
      .andThen {
        case Failure(_) => subscription.cancel()
      }

  }

  /** Second read, to ensure closing race condition gap. */
  protected def readAgain(id: ID, minRevision: Int, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]]

  private def readMin(
      id: ID, timeout: FiniteDuration, minRevision: Int = -1, minTick: Long = Long.MinValue)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = {

      def expected(revision: Int, tick: Long): Boolean = {
        if (revision < 0 && minRevision >= 0) {
          throw new IllegalArgumentException(s"Snapshot revision is not supported (possibly because of joined streams). Use `tick` instead.")
        }
        revision >= minRevision && tick >= minTick
      }

      def readAgain(
          id: ID, minRevision: Int, minTick: Long, promise: Promise[Snapshot]): Unit = {

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
          val promise = Promise[Snapshot]
          val subscription = this.subscribe(id) { update =>
            if (expected(update.revision, update.tick)) {
              val snapshotRev = maybeSnapshot.map(_.revision) getOrElse -1
              val updSnapshot = {
                if (snapshotRev != update.revision - 1) None
                else update.changed match {
                  case Some(updated) =>
                    updateState(id, maybeSnapshot.map(_.content), updated)
                      .map(s => new Snapshot(s, update.revision, update.tick))
                  case _ =>
                    maybeSnapshot
                      .map(_.copy(revision = update.revision, tick = update.tick))
                }
              }
              updSnapshot match {
                case Some(snapshot) =>
                  promise trySuccess snapshot
                case None =>
                  readAgain(id, minRevision, minTick, promise)
              }
            }
          }
          promise.future.onComplete(_ => subscription.cancel)
          scheduler.schedule(timeout) {
            if (!promise.isCompleted) {
              promise tryFailure Timeout(id, maybeSnapshot, minRevision, minTick, timeout)
            }
          }
          // Unfortunately we have to try another read, to eliminate the race condition
          readAgain(id, minRevision, minTick, promise)
          promise.future
        }
    }
  }

}

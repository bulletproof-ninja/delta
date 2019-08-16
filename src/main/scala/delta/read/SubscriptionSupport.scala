package delta.read

import scala.util.{ Success, Failure }
import scala.concurrent._, duration.FiniteDuration
import scuff.concurrent._
import scuff.Subscription
import java.util.concurrent.{ CountDownLatch, ScheduledExecutorService }
import java.util.concurrent.atomic.AtomicLong

trait SubscriptionSupport[ID, S] {
  rm: BasicReadModel[ID, S] =>

  type SnapshotUpdate = delta.process.SnapshotUpdate[S]

  /** Timeout scheduler. */
  protected def scheduler: ScheduledExecutorService
  /** Snapshot update subscription stub. */
  protected def subscribe(id: ID)(pf: PartialFunction[SnapshotUpdate, Unit]): Subscription

  /**
   * Read snapshot, ensuring it's at least the given revision,
   * waiting the supplied timeout if current revision is stale.
   * @param id The read identifier
   * @param minRevision The minimum revision to return
   * @param timeout Read timeout. This timeout is only used if `minRevision` doesn't match
   * @return Snapshot >= `minRevision` or [[delta.read.ReadRequestFailure]] cannot fullfil
   */
  def readMinRevision(id: ID, minRevision: Int, timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readOrSubscribe(id, Right(minRevision), timeout)

  /**
   * Read snapshot, ensuring it's at least the given tick,
   * waiting the supplied timeout if current is too old.
   * @param id The read identifier
   * @param afterTick The tick value the snapshot must succeed
   * @param timeout Lookup timeout. This timeout is only used if `afterTick` doesn't match
   * @return Snapshot >= `minTick` or [[delta.read.ReadRequestFailure]] cannot fullfil
   */
  def readMinTick(id: ID, minTick: Long, timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readOrSubscribe(id, Left(minTick), timeout)

  private final class Callback(id: ID, snapshot: Either[Snapshot, SnapshotUpdate], callback: Either[Snapshot, SnapshotUpdate] => Unit)
    extends Runnable {
    def run = callback(snapshot)
    override def hashCode = id.##
  }

  /**
   * Subscribe, with initial lookup. The built-in initial lookup eliminates
   * any potential race conditions otherwise possible, if not handled specifically.
   * @param id The subscription identifier (same as lookup identifier)
   * @param callbackCtx The execution context used for callback. Use single-thread to ensure ordering, if required
   * @param callback The snapshot callback, either `Left` for initial snapshot, or `Right` for subsequent updates.
   * @return Subscription. The future resolves once the initial lookup has been completed.
   */
  def subscribe(id: ID, callbackCtx: ExecutionContext)(callback: Either[Snapshot, SnapshotUpdate] => Unit): Future[Subscription] = {

    val firstTickSeen = new AtomicLong(Long.MinValue)
    val firstCallbackLatch = new CountDownLatch(1)
      def doCallback(snapshot: Either[Snapshot, SnapshotUpdate], tick: Long): Unit = {
        val firstSeen = firstTickSeen.get
        if (firstSeen == Long.MinValue) { // First snapshot:
          if (!firstTickSeen.compareAndSet(Long.MinValue, tick)) doCallback(snapshot, tick)
          else try {
            val first = snapshot match {
              case first @ Left(_) => first
              case Right(delta.process.SnapshotUpdate(snapshot, _)) => Left(snapshot)
            }
            callbackCtx execute new Callback(id, first, callback)
          } finally firstCallbackLatch.countDown()
        } else { // Only updates after first:
          if (snapshot.isRight && tick > firstSeen) {
            if (firstCallbackLatch.getCount == 0) callbackCtx execute new Callback(id, snapshot, callback)
            else {
              // first seen tick has already been updated,
              // so we only ever hit this during a race condition,
              // which is guaranteed to be resolved.
              blocking(firstCallbackLatch.await)
              doCallback(snapshot, tick)
            }
          }
        }
      }

    val subscription = this.subscribe(id) {
      case update =>
        doCallback(Right(update), update.snapshot.tick)
    }
    // Piggy-back on reader thread. Don't use potentially single threaded callbackCtx, which could possibly deadlock.
    readLatest(id)(Threads.PiggyBack).map { snapshot =>
      doCallback(Left(snapshot), snapshot.tick)
      subscription
    }(Threads.PiggyBack)
  }

  protected def readStrict(id: ID, tickOrRevision: Either[Long, Int])(
      implicit
      ec: ExecutionContext): Future[Snapshot]

  private def readOrSubscribe(id: ID, tickOrRevision: Either[Long, Int], timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = {
      def matchesTickOrRevision(snapshot: Snapshot): Boolean = tickOrRevision match {
        case Right(minRevision) =>
          if (snapshot.revision < 0) throw new IllegalArgumentException(s"Snapshots do not support revision (possibly because of joined streams). Use `tick` instead.")
          snapshot.revision >= minRevision
        case Left(minTick) => snapshot.tick >= minTick
      }
    val latest = readLatest(id).map(Some(_)).recover { case _: UnknownIdRequested => None }
    latest flatMap {
      case Some(snapshot) if matchesTickOrRevision(snapshot) =>
        Future successful snapshot
      case maybeSnapshot => // Earlier revision or tick than expected, or unknown id
        if (timeout.length == 0) {
          Future failed Timeout(id, maybeSnapshot, tickOrRevision, timeout)
        } else {
          val promise = Promise[Snapshot]
          val subscription = this.subscribe(id) {
            case delta.process.SnapshotUpdate(snapshot, _) if matchesTickOrRevision(snapshot) =>
              promise trySuccess snapshot
          }
          promise.future.onComplete(_ => subscription.cancel)
          scheduler.schedule(timeout) {
            if (!promise.isCompleted) {
              promise tryFailure Timeout(id, maybeSnapshot, tickOrRevision, timeout)
            }
          }
          // Unfortunately we have to try another read, to eliminate the race condition
          readStrict(id, tickOrRevision) andThen {
            case Success(snapshot) if matchesTickOrRevision(snapshot) =>
              promise trySuccess snapshot
            case Failure(th) =>
              promise tryFailure th
          }
          promise.future
        }
    }
  }

}

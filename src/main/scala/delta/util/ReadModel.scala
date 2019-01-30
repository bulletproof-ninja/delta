package delta.util

import java.util.concurrent.{ CountDownLatch, ScheduledExecutorService }
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.{ ExecutionContext, Future, Promise, blocking }
import scala.concurrent.duration.FiniteDuration

import scuff.Subscription
import scuff.concurrent._
import scala.util.control.NoStackTrace
import scala.util.Success
import scala.util.Failure

trait ReadModel[ID, S] {

  protected type SnapshotUpdate = delta.util.SnapshotUpdate[S]
  protected type Snapshot = delta.Snapshot[S]

  /** Scheduler used for timeouts. */
  protected def scheduler: ScheduledExecutorService

  /** Report exceptions. */
  protected def reportFailure(th: Throwable): Unit

  /** Implementation of snapshot read. */
  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]]

  /** Implementation of snapshot update subscription. */
  protected def subscribe(id: ID)(pf: PartialFunction[SnapshotUpdate, Unit]): Subscription

  protected def defaultReadTimeout: FiniteDuration = ReadModel.DefaultReadTimeout

  /** Read current snapshot, if exists. */
  def readCurrent(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] = readSnapshot(id)

  /**
   * Read snapshot after tick, waiting the default timeout if current is too old.
   * @param id The lookup identifier
   * @param afterTick The tick value the snapshot must succeed
   * @return Snapshot younger than `afterTick` or `TimeoutException` if timeout expires
   */
  def readAfterTick(id: ID, afterTick: Long)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readOrSubscribe(id, Left(afterTick), defaultReadTimeout)

  /**
   * Read snapshot after tick, waiting the supplied timeout if current is too old.
   * @param id The read identifier
   * @param afterTick The tick value the snapshot must succeed
   * @param timeout Lookup timeout. This timeout is only used if `afterTick` doesn't match
   * @return Snapshot younger than `afterTick` or `TimeoutException` if timeout expires
   */
  def readAfterTick(id: ID, afterTick: Long, timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readOrSubscribe(id, Left(afterTick), timeout)

  /**
   * Lookup by revision, waiting the default timeout if current revision is stale.
   * @param id The lookup identifier
   * @param minRevision The minimum revision to lookup
   * @return Snapshot with at least `minRevision` or `TimeoutException` if timeout expires
   */
  def readFromRevision(id: ID, minRevision: Int)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readOrSubscribe(id, Right(minRevision), defaultReadTimeout)

  /**
   * Read from minimum revision, waiting the supplied timeout if current revision is stale.
   * @param id The read identifier
   * @param minRevision The minimum revision to return
   * @param timeout Read timeout. This timeout is only used if `minRevision` doesn't match
   * @return Snapshot with at least `minRevision` or `TimeoutException` if timeout expires
   */
  def readFromRevision(id: ID, minRevision: Int, timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readOrSubscribe(id, Right(minRevision), timeout)

  private def readOrSubscribe(id: ID, tickOrRevision: Either[Long, Int], timeout: FiniteDuration)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = {
      def matchesTickOrRevision(snapshot: Snapshot): Boolean = tickOrRevision match {
        case Right(minRevision) =>
          if (snapshot.revision < 0) throw new IllegalArgumentException(s"Snapshots do not support revision (possibly built by ${classOf[delta.util.JoinState[_, _, _]].getName}). Use `tick` instead.")
          snapshot.revision >= minRevision
        case Left(afterTick) => snapshot.tick > afterTick
      }
      def newTimeoutException(): ReadModel.TimeoutException = {
        val specific = tickOrRevision match {
          case Left(afterTick) => s"tick > $afterTick"
          case Right(minRevision) => s"revision >= $minRevision"
        }
        new ReadModel.TimeoutException(id, tickOrRevision, timeout)(s"Failed to find $id with $specific, within timeout of $timeout") 
      }
    readSnapshot(id) flatMap {
      case Some(snapshot) if matchesTickOrRevision(snapshot) =>
        Future successful snapshot
      case _ => // Earlier revision or tick than expected, or None at all
        if (timeout.length == 0) {
          Future failed newTimeoutException()
        } else {
          val promise = Promise[Snapshot]
          val subscription = this.subscribe(id) {
            case SnapshotUpdate(snapshot, _) if matchesTickOrRevision(snapshot) =>
              promise trySuccess snapshot
          }
          promise.future.onComplete(_ => subscription.cancel)
          scheduler.schedule(timeout) {
            if (!promise.isCompleted) {
              promise tryFailure newTimeoutException()
            }
          }
          readSnapshot(id) andThen { // Unfortunately we have to try another read, to eliminate the race condition
            case Success(Some(snapshot)) if matchesTickOrRevision(snapshot) =>
              promise trySuccess snapshot
            case Failure(th) =>
              promise tryFailure th
          }
          promise.future
        }
    }
  }

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
              case Right(SnapshotUpdate( /*_,*/ snapshot, _)) => Left(snapshot)
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
    readSnapshot(id)(Threads.PiggyBack).map { option =>
      option.foreach(snapshot => doCallback(Left(snapshot), snapshot.tick))
      subscription
    }(Threads.PiggyBack)
  }

}

object ReadModel {

  import concurrent.duration._

  val DefaultReadTimeout = 5555.millis

  final case class TimeoutException(id: Any, tickOrRevision: Either[Long, Int], timeout: FiniteDuration)(msg: String)
    extends java.util.concurrent.TimeoutException(msg) with NoStackTrace

  trait MessageHubSupport[ID, S] {
    rm: ReadModel[ID, S] =>

    protected def updateHub: delta.MessageHub[(ID, SnapshotUpdate)]
    protected def updateHubNamespace: delta.MessageHub.Namespace

    protected def subscribe(MatchId: ID)(pf: PartialFunction[SnapshotUpdate, Unit]): Subscription = {
      updateHub.subscribe(updateHubNamespace) {
        case (MatchId, update) if (pf isDefinedAt update) => pf(update)
      }
    }

  }

  trait SnapshotStoreSupport[ID, SS, S >: SS] {
    rm: ReadModel[ID, S] =>

    protected def stateClass: Class[S]
    protected def snapshotStore: delta.SnapshotStore[ID, SS]
    protected def readSnapshot(id: ID)(implicit ec: ExecutionContext): Future[Option[delta.Snapshot[S]]] = {
      snapshotStore.read(id).map {
        _.collect {
          case snapshot if stateClass.isInstance(snapshot.content) => snapshot.asInstanceOf[Snapshot]
        }
      }
    }

  }

}

package delta.util

import scuff.concurrent._
import scala.reflect.ClassTag
import scala.collection.concurrent.TrieMap
import java.util.concurrent.ScheduledFuture
import scuff.StreamConsumer
import scala.concurrent.duration.FiniteDuration
import delta.EventSource
import java.util.concurrent.ScheduledExecutorService

abstract class DefaultMonotonicProcessor[ID, EVT: ClassTag, S >: Null](
    es: EventSource[ID, _ >: EVT],
    store: StreamProcessStore[ID, S],
    partitionThreads: PartitionedExecutionContext,
    replayMissingRevisionsDelay: FiniteDuration,
    scheduler: ScheduledExecutorService)
  extends MonotonicProcessor[ID, EVT, S](store) {

  protected def executionContext(id: ID) = partitionThreads.singleThread(id.hashCode)

  private[this] val outstandingReplays = new TrieMap[ID, (Range, ScheduledFuture[_])]
  protected def onMissingRevisions(id: ID, missing: Range): Unit = {
    val missingAdjusted: Range = outstandingReplays.lookup(id) match {
      case null => missing
      case existing @ (outstandingReplay, schedule) =>
        if (missing == outstandingReplay) {
          0 until 0 // Empty, because already scheduled
        } else { // range can only be smaller, thus resubmit with tighter range
          if (schedule.cancel(false)) { // Cancelled
            outstandingReplays.remove(id, existing)
            missing
          } else { // Canceling failed
            0 until 0 // Empty, because already replayed
          }
        }
    }
    scheduleRevisionsReplay(id, missingAdjusted)(partitionThreads.reportFailure)
  }

  private def scheduleRevisionsReplay(id: ID, missing: Range)(reportFailure: Throwable => Unit): Unit = if (missing.nonEmpty) {
    val replayConsumer = new StreamConsumer[TXN, Unit] {
      def onNext(txn: TXN) = apply(txn)
      def onError(th: Throwable) = {
        reportFailure(th)
        onDone()
      }
      def onDone() = {
        outstandingReplays.lookup(id) match {
          case value @ (range, _) if range == missing =>
            outstandingReplays.remove(id, value)
          case _ => // Already removed
        }
      }
    }
    val replaySchedule = scheduler.schedule(replayMissingRevisionsDelay) {
      es.replayStreamRange(id, missing)(replayConsumer)
    }
    outstandingReplays.update(id, missing -> replaySchedule)
  }

}

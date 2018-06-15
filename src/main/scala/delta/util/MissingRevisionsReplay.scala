package delta.util

import scuff.concurrent._
import scala.collection.concurrent.TrieMap
import java.util.concurrent.ScheduledFuture
import scuff.StreamConsumer
import scala.concurrent.duration.FiniteDuration
import delta.EventSource
import java.util.concurrent.ScheduledExecutorService
import delta.Transaction

trait MissingRevisionsReplay[ID, EVT] {

  private[this] val outstandingReplays = new TrieMap[ID, (Range, ScheduledFuture[_])]
  protected def onMissingRevisions(
      es: EventSource[ID, _ >: EVT], scheduler: ScheduledExecutorService, reportFailure: Throwable => Unit)(
      id: ID, missing: Range, replayDelay: FiniteDuration)(replayProcess: Transaction[ID, _ >: EVT] => _): Unit = {
    val missingAdjusted: Option[Range] = outstandingReplays.lookup(id) match {
      case null => Some(missing)
      case existing @ (outstandingReplay, schedule) =>
        if (missing == outstandingReplay) {
          None // Already scheduled
        } else { // range can only be smaller, thus resubmit with tighter range
          if (schedule.cancel(false)) { // Cancelled
            outstandingReplays.remove(id, existing)
            Some(missing)
          } else { // Canceling failed
            None // Already replayed
          }
        }
    }
    missingAdjusted.foreach {
      scheduleRevisionsReplay(id, _, es, scheduler, replayDelay, replayProcess, reportFailure)
    }
  }

  private type TXN = Transaction[ID, _ >: EVT]

  private def scheduleRevisionsReplay(id: ID, missing: Range, es: EventSource[ID, _ >: EVT], scheduler: ScheduledExecutorService, replayDelay: FiniteDuration, replayProcess: TXN => _, reportFailure: Throwable => Unit): Unit = {
    val replayConsumer = new StreamConsumer[TXN, Unit] {
      def onNext(txn: TXN) = replayProcess(txn)
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
    val replaySchedule = scheduler.schedule(replayDelay) {
      es.replayStreamRange(id, missing)(replayConsumer)
    }
    outstandingReplays.update(id, missing -> replaySchedule)
  }

}

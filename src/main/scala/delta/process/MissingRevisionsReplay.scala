package delta.process

import scuff.concurrent._
import scala.collection.concurrent.TrieMap
import java.util.concurrent.ScheduledFuture
import scuff.StreamConsumer
import scala.concurrent.duration.FiniteDuration
import delta.EventSource
import java.util.concurrent.ScheduledExecutorService

/**
 * Request replay of transactions from [[delta.EventSource]]
 * when revisions are missing.
 */
trait MissingRevisionsReplay[ID, EVT] {

  private type Transaction = delta.Transaction[ID, _ >: EVT]

  private[this] val outstandingReplays = new TrieMap[ID, (Range, ScheduledFuture[_])]

  protected def replayMissingRevisions(
      es: EventSource[ID, _ >: EVT],
      replayDelay: FiniteDuration,
      scheduler: ScheduledExecutorService,
      reportFailure: Throwable => Unit)(
      id: ID, missing: Range)(replayProcess: Transaction => _): Unit =
    if (!(outstandingReplays contains id)) {
      scheduleRevisionsReplay(id, missing, es, scheduler, replayDelay, reportFailure, replayProcess)
    }

  private def scheduleRevisionsReplay(id: ID, missing: Range, es: EventSource[ID, _ >: EVT], scheduler: ScheduledExecutorService, replayDelay: FiniteDuration, reportFailure: Throwable => Unit, replayProcess: Transaction => _): Unit =
    if (replayDelay.length == 0) {
      replayNow(id, missing, es, reportFailure, replayProcess)(())
    } else {
      val replaySchedule = scheduler.schedule(replayDelay) {
        replayNow(id, missing, es, reportFailure, replayProcess) {
          // onDone:
          outstandingReplays.getOrElse(id, null) match {
            case value @ (range, _) if range == missing =>
              outstandingReplays.remove(id, value)
            case _ => // Already removed
          }
        }
      }
      if (outstandingReplays.putIfAbsent(id, missing -> replaySchedule).isDefined) { // Race condition:
        replaySchedule.cancel(false)
      }
    }

  private def replayNow(
      id: ID, missing: Range, es: EventSource[ID, _ >: EVT],
      reportFailure: Throwable => Unit, replayProcess: Transaction => _)(whenDone: => Unit): Unit = {
    val replayConsumer = new StreamConsumer[Transaction, Unit] {
      def onNext(tx: Transaction) = replayProcess(tx)
      def onError(th: Throwable) = {
        reportFailure(th)
        onDone()
      }
      def onDone() = whenDone
    }
    es.replayStreamRange(id, missing)(replayConsumer)
  }

}

class MissingRevisionsReplayer[ID, EVT](
  es: EventSource[ID, _ >: EVT],
  replayDelay: FiniteDuration,
  scheduler: ScheduledExecutorService,
  reportFailure: Throwable => Unit)
extends MissingRevisionsReplay[ID, EVT] {

  private[this] val scheduleReplay = replayMissingRevisions(es, replayDelay, scheduler, reportFailure) _

  def scheduleMissingRevisionsReplay(
      id: ID, missing: Range)(replayProcess: delta.Transaction[ID, _ >: EVT] => _): Unit =
    scheduleReplay(id, missing)(replayProcess)

}

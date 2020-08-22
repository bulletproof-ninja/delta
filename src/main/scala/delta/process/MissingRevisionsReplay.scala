package delta.process

import delta.EventSource

import scuff.StreamConsumer
import scuff.concurrent._

import scala.collection.concurrent.TrieMap
import scala.concurrent._, duration.FiniteDuration

import java.util.concurrent.ScheduledFuture
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
      delayReplay: Option[(FiniteDuration, ScheduledExecutorService)])(
      id: ID, missing: Range)(replayProcess: Transaction => _): Future[Unit] =
    if (!(outstandingReplays contains id)) {
      scheduleRevisionsReplay(id, missing, es, delayReplay, replayProcess)
    } else Future.unit

  private def scheduleRevisionsReplay(
      id: ID, missing: Range, es: EventSource[ID, _ >: EVT],
      delayReplay: Option[(FiniteDuration, ScheduledExecutorService)],
      replayProcess: Transaction => _): Future[Unit] = {

    val promise = Promise[Unit]()

    delayReplay match {
      case None =>
        replayNow(id, missing, es, replayProcess, promise)
      case Some((delay, _)) if delay.length == 0 =>
        replayNow(id, missing, es, replayProcess, promise)
      case Some((delay, scheduler)) =>
        val replaySchedule = scheduler.schedule(delay) {
          replayNow(id, missing, es, replayProcess, promise)
          promise.future.andThen { _ =>
            outstandingReplays.getOrElse(id, null) match {
              case value @ (range, _) if range == missing =>
                outstandingReplays.remove(id, value)
              case _ => // Already removed
            }
          }(Threads.PiggyBack)
        }
        if (outstandingReplays.putIfAbsent(id, missing -> replaySchedule).isDefined) { // Race condition:
          replaySchedule.cancel(false)
        }
    }

    promise.future
  }

  private def replayNow(
      id: ID, missing: Range, es: EventSource[ID, _ >: EVT],
      replayProcess: Transaction => _, whenDone: Promise[Unit]): Unit = {
    val replayConsumer = new StreamConsumer[Transaction, Unit] {
      def onNext(tx: Transaction) = replayProcess(tx)
      def onError(th: Throwable) = whenDone failure th
      def onDone() = whenDone success ()
    }
    es.replayStreamRange(id, missing)(replayConsumer)
  }

}

class MissingRevisionsReplayer[ID, EVT](
  es: EventSource[ID, _ >: EVT],
  replayDelaySchedule: Option[(FiniteDuration, ScheduledExecutorService)] = None)
extends MissingRevisionsReplay[ID, EVT] {

  private[this] val requestMissingReplay = replayMissingRevisions(es, replayDelaySchedule) _

  def requestReplay(
      id: ID, missing: Range)(replayProcess: delta.Transaction[ID, _ >: EVT] => _): Future[Unit] =
    requestMissingReplay(id, missing)(replayProcess)

}

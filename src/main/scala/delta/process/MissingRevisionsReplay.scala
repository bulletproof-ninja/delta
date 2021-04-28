package delta.process

import delta.{ Revision, EventSource }
import delta.process.LiveProcessConfig._

import scuff.Reduction
import scuff.concurrent._

import scala.collection.concurrent.TrieMap
import scala.concurrent._

import java.util.concurrent.ScheduledFuture
import scala.util._
import scala.concurrent.duration.FiniteDuration

/**
 * Request replay of transactions from [[delta.EventSource]]
 * when revisions are missing.
 */
trait MissingRevisionsReplay[ID, EVT] {

  private type Transaction = delta.Transaction[ID, _ >: EVT]

  private[this] val outstandingReplays = new TrieMap[ID, (Range, ScheduledFuture[_])]

  protected def replayMissingRevisions(
      es: EventSource[ID, _ >: EVT],
      config: OnMissingRevision)(
      id: ID, missing: Range)(replayProcess: Transaction => Any): Future[Unit] =
    if (!(outstandingReplays contains id)) {
      scheduleRevisionsReplay(id, missing, es, config, replayProcess)
    } else Future.unit

  private def scheduleRevisionsReplay(
      id: ID, missing: Range, es: EventSource[ID, _ >: EVT],
      config: OnMissingRevision,
      replayProcess: Transaction => _): Future[Unit] = {

    val promise = Promise[Unit]()

    config match {
      case DelayedReplay(delay, scheduler) =>
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
      case ImmediateReplay =>
        replayNow(id, missing, es, replayProcess, promise)
      case Ignore =>
        // Ignoring
    }

    promise.future

  }

  private def replayNow(
      id: ID, missing: Range, es: EventSource[ID, _ >: EVT],
      replayProcessor: Transaction => Any, replayPromise: Promise[Unit]): Unit = {

    val replayConsumer = new AsyncReduction[Transaction, Unit] {

      override protected def resultTimeout: FiniteDuration = ???

      override protected def asyncResult(timedOut: Option[TimeoutException], errors: List[(Transaction, Throwable)]): Future[Unit] = ???

      override def asyncNext(t: Transaction): Future[Any] = ???

      // @volatile private[this] var txs: List[Transaction] = Nil
      // def next(tx: Transaction) =
      //   txs = tx :: txs
      // def result(): Future[Unit] = {
      //   txs
      //     .map(tx => Try(replayProcessor(tx)))
      //     .collectFirst {
      //       case Success(f: Future[_]) => f.map { _ => () } (Threads.PiggyBack)
      //       case tr => Future fromTry tr.map { _ => () }
      //     } match {
      //       case None =>
      //         replayPromise success ()
      //       case Some(future) =>
      //         replayPromise completeWith future
      //     }
      // }
    }
    if (missing.last == Int.MaxValue)
      es.replayStreamFrom(id, missing.head)(replayConsumer)
    else
      es.replayStreamRange(id, missing)(replayConsumer)
  }

}

class MissingRevisionsReplayer[ID, EVT](
  es: EventSource[ID, _ >: EVT],
  config: OnMissingRevision = ImmediateReplay)
extends MissingRevisionsReplay[ID, EVT] {

  private[this] val requestMissingReplay = replayMissingRevisions(es, config) _

  def requestReplay(
      id: ID, missing: Range)(replayProcess: delta.Transaction[ID, _ >: EVT] => _): Future[Unit] =
    requestMissingReplay(id, missing)(replayProcess)

  def requestReplayFrom(
      id: ID, from: Revision)(replayProcess: delta.Transaction[ID, _ >: EVT] => _): Future[Unit] =
    requestMissingReplay(id, from to Int.MaxValue)(replayProcess)

}

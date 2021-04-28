package delta.process

// import scala.collection.compat._
import scala.concurrent._, duration._

import scuff.concurrent._

/**
  * General trait for consuming an [[delta.EventSource]].
  * Consumption is two-phased:
  * 1. Replay phase: Transactions are replayed to catch up with current state
  * 2. Live phase: Transactions are consumed as published
  *
  * @see [[delta.process.IdempotentConsumer]] as the recommended
  * default implementation.
  * @tparam SID The stream id type
  * @tparam EVT The processing event type. Can be a sub-type of the evemt source event type
  */
trait EventSourceConsumer[SID, EVT]
extends EventSourceProcessing[SID, EVT] {

  /**
   * Start consumption of transactions, either from the
   * very beginning or from the tick watermark.
   * @param eventSource The [[delta.EventSource]] to process.
   * @return The replay process, which contains ongoing replay numbers and a future live process,
   * which becomes available when replay processing is finished.
   */
  def start(
      eventSource: EventSource,
      replayConfig: ReplayProcessConfig,
      liveConfig: LiveProcessConfig)(
      implicit
      ec: ExecutionContext)
      : ReplayProcess[(ReplayResult[SID], LiveProcess)] = {

    val selector = this.selector(eventSource)

    val maxTickBeforeReplay = eventSource.maxTick.await(60.seconds)
    val replayProcess = this.catchUp(eventSource, maxTickBeforeReplay, replayConfig)
    val liveProc = replayProcess.finished.flatMap { replayCompletion =>
      val liveProcessor = this.liveProcessor(eventSource, liveConfig)
      completeStreams(eventSource, replayCompletion.brokenStreams, liveProcessor, replayConfig.completionTimeout)
        .flatMap { streamErrors =>
          val liveSubscription = eventSource.subscribeGlobal(selector.toStreamsSelector)(liveProcessor)
          // close window for potential race condition
          // with subscription, by re-querying anything
          // since starting replay.
          val windowClosed: Future[Unit] = {
            val consumer = new AsyncReduction[Transaction, Unit] {
              def resultTimeout = replayConfig.completionTimeout
              def asyncNext(tx: Transaction) = liveProcessor(tx)
              def asyncResult(
                  timeout: Option[TimeoutException],
                  processErrors: List[(Transaction, Throwable)]) = {
                timeout match {
                  case Some(timeout) => Future failed timeout
                  case None =>
                    processErrors.foreach {
                      case (tx, cause) =>
                        ec reportFailure new IllegalStateException(
                          s"Stream `${tx.stream}`, revision ${tx.revision}, failed to process correctly", cause)
                    }
                }
                Future.unit
              }
            }
            replayConfig.adjustToWindow(maxTickBeforeReplay) match {
              case None =>
                eventSource.query(selector)(consumer)
              case Some(fromTick) =>
                eventSource.querySince(fromTick, selector)(consumer)
            }
          }
          windowClosed.map { _ =>
            ReplayResult(replayCompletion.txCount, streamErrors) ->
              new LiveProcess {
                def cancel() = liveSubscription.cancel()
                def name = replayProcess.name
              }
          }
        }
    }

    ReplayProcess(replayProcess, liveProc)

  }

}

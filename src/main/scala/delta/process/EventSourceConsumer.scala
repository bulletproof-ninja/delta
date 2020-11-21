package delta.process

import scala.collection.compat._
import scala.concurrent._, duration._
import scala.util.{ Success, Failure }

import scuff.concurrent._

import delta.process.ReplayCompletion.IncompleteStream

/**
  * General trait for consuming an [[delta.EventSource]].
  * @see [[delta.process.PersistentMonotonicConsumer]] as the recommended
  * default implementation.
  * @tparam SID The stream id type
  * @tparam EVT The processing event type. Can be a sub-type of the evemt source event type
  */
trait EventSourceConsumer[SID, EVT]
extends EventSourceProcessing[SID, EVT] {

  /**
   * Start consumption of transactions, either from the
   * very beginning or from the tick watermark.
   * @param eventSource The [[EventSource]] to process.
   * @return The replay process, which contains ongoing replay numbers and a future live process,
   * which becomes available when replay processing is finished.
   */
  def consume(
      eventSource: EventSource,
      replayConfig: ReplayProcessConfig,
      liveConfig: LiveProcessConfig)(
      implicit
      ec: ExecutionContext): ReplayProcess[(ReplayResult[SID], LiveProcess)] = {

    val selector = this.selector(eventSource)

    val tickBeforeReplay = eventSource.maxTick.await(10.seconds)
    val (replayStatus, replayFinish) = this.catchUp(eventSource, replayConfig)
    val liveProc = replayFinish.flatMap { replayCompletion =>
      val liveProcessor = this.liveProcessor(eventSource, liveConfig)
      val completionReplays = completeStreams(
        eventSource, replayCompletion.incompleteStreams, liveProcessor)

      (Future sequence completionReplays.values).transformWith { _ =>
        val liveSubscription = eventSource.subscribe(selector.toStreamsSelector)(liveProcessor)
        // close window for potential race condition
        // with subscription, by re-querying anything
        // since starting replay.
        val windowClosed =
          replayConfig.adjustToWindow(tickBeforeReplay) match {
            case None =>
              val query = eventSource.query(selector) _
              StreamPromise.foreach(query)(liveProcessor)
            case Some(fromTick) =>
              val windowQuery = eventSource.querySince(fromTick, selector) _
              StreamPromise.foreach(windowQuery)(liveProcessor)
          }

        windowClosed.map { _ =>
          val streamErrors =
            completionReplays.view
              .mapValues(_.value)
              .collect {
                case (id, Some(Failure(failure))) => id -> failure
              }
              .toMap
          ReplayResult(replayCompletion.txCount, streamErrors) ->
            new LiveProcess {
              def cancel() = liveSubscription.cancel()
              def name = replayStatus.name
            }
        }
      }
    }

    ReplayProcess(replayStatus, liveProc)

  }

}

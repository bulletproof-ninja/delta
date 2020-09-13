package delta.process

import scuff.concurrent._

import scala.concurrent._, duration._

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
  def consume(eventSource: EventSource)(
      implicit
      ec: ExecutionContext): ReplayProcess[LiveProcess] = {

    val selector = this.selector(eventSource)
    val tickWindow = this.tickWindow
    require(tickWindow >= 0, s"Cannot have negative tick window: $tickWindow")

    val tickAtStart = eventSource.maxTick.await(33.seconds)
    val (replayStatus, replayFinish) = this.catchUp(eventSource)
    val liveProc = replayFinish.flatMap { _ =>
        val liveProcessor = this.liveProcessor(eventSource)
        val liveSubscription = eventSource.subscribe(selector.toStreamsSelector)(liveProcessor)
        // close window for potential race condition
        // with subscription, by re-querying anything
        // since starting replay.
        fromTick(tickAtStart) match {
          case Long.MinValue =>
            val query = eventSource.query(selector) _
            StreamPromise.foreach(query)(liveProcessor)
          case fromTick =>
            val windowQuery = eventSource.querySince(fromTick, selector) _
            StreamPromise.foreach(windowQuery)(liveProcessor)
        }
        val windowClosed = tickAtStart match {
          case None =>
            val query = eventSource.query(selector) _
            StreamPromise.foreach(query)(liveProcessor)
          case Some(tickAtStart) =>
            val windowQuery = eventSource.querySince(tickAtStart - tickWindow, selector) _
            StreamPromise.foreach(windowQuery)(liveProcessor)
        }
        windowClosed.map { _ =>
          new LiveProcess {
            def cancel() = liveSubscription.cancel()
            def name = replayStatus.name
          }
        }
    }

    ReplayProcess(replayStatus, liveProc)

  }

}

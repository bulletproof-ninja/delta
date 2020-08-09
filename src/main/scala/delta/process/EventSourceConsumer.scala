package delta.process

import scuff.concurrent.StreamPromise

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scuff.Subscription

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
   * @return A future subscription to live events.
   * This will be available when replay processing is done,
   * thus state is considered current.
   */
  def consume(eventSource: EventSource)(
      implicit
      ec: ExecutionContext): Future[Subscription] = {

    val selector = this.selector(eventSource)
    val tickWindow = this.tickWindow
    require(tickWindow >= 0, s"Cannot have negative tick window: $tickWindow")

    eventSource.maxTick.flatMap { tickAtStart =>
      this.catchUp(eventSource).flatMap { _ =>
        val liveProcessor = this.liveProcessor(eventSource)
        val liveSubscription = eventSource.subscribe(selector.toStreamsSelector)(liveProcessor)
        // close window for potential race condition
        // with subscription, by re-querying anything
        // since starting replay.
        val windowClosed = tickAtStart match {
          case None =>
            val query = eventSource.query(selector) _
            StreamPromise.foreach(query)(liveProcessor)
          case Some(tickAtStart) =>
            val windowQuery = eventSource.querySince(tickAtStart - tickWindow, selector) _
            StreamPromise.foreach(windowQuery)(liveProcessor)
        }
        windowClosed.map(_ => liveSubscription)
      }
    }
  }


}

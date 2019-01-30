package delta.util

import scala.concurrent.{ ExecutionContext, Future }

import delta.EventSource
import scuff.Subscription
import scuff.StreamConsumer
import scuff.concurrent.StreamPromise
import scuff.concurrent.Threads
import delta.Transaction

/**
  * A persistent event source consumer, consisting of
  * replay and live transaction processors, useful for
  * building read models and other transaction processors,
  * e.g. sagas.
  * Can be started and stopped on demand, without loss of
  * transactions.
  * @param maxTickProcessed The highest tick processed.
  * NOTE: Since replay processing is generally
  * not done in tick order, a tick received
  * through incomplete replay processing
  * is NOT reliable as a high-water mark. Any
  * tick received in live processing can
  * be considered a high-water mark, and so
  * can ticks from completed replay processing.
  */
trait EventSourceConsumer[ID, EVT] {

  protected type ReplayResult

  type ES = EventSource[ID, _ >: EVT]
  protected type TXN = Transaction[ID, _ >: EVT]

  /** Transaction selector. */
  protected def selector(es: ES): es.Selector

  /**
    * Called at startup, when replay processing of
    * transactions begins. Unlike live processing,
    * there will be no duplicate revisions passed
    * to replay processor.
    *
    * Replay processing enables in-memory processing,
    * where persistence can be delayed until done,
    * making large data sets much faster to process.
    *
    * NOTE: Tick order is arbitrary, thus no guarantee
    * of causal tick processing, between different streams,
    * although, as mentioned, individual streams will be
    * processed in causal (monotonic revision) order.
    * If the event source is empty, this method will
    * not be called.
    *
    * It is highly recommended to return an instance of
    * [[delta.util.MonotonicReplayProcessor]] here.
    */
  protected def replayProcessor(es: ES): StreamConsumer[TXN, Future[ReplayResult]]

  /**
    * When replay processing is completed, a live processor
    * is requested.
    * A live processor must take care to handle these
    * situations:
    *     1) Out-of-order revisions
    *     2) Duplicate revisions
    *     3) Potential concurrent calls
    *
    * It is highly recommended to return an instance of
    * [[delta.util.MonotonicProcessor]] here,
    * which will handle all 3 above cases.
    *
    * @param es The [[delta.EventSource]] being processed.
    * @param replayResult The result of replay processing, if any.
    */
  protected def liveProcessor(es: ES, replayResult: Option[ReplayResult]): TXN => _

  /** The currently processed tick watermark. */
  protected def tickWatermark: Option[Long]

  /**
    * Start consumption of transactions, either from beginning
    * of time, or from the tick watermark.
    * @param eventSource The [[EventSource]] to process.
    * @param maxTickSkew The max tick skew, i.e. the largest reasonably possible wall-clock skew or network propagation delay, whichever is larger
    * @return A future subscription to live events.
    * This will be available when historic processing is done,
    * thus state is considered current.
    */
  def consume(eventSource: ES, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    require(maxTickSkew >= 0, s"Cannot have negative tick skew: $maxTickSkew")
    tickWatermark match {
      case None =>
        eventSource.maxTick().flatMap { maxEventSourceTick =>
          start(eventSource, maxEventSourceTick)(selector(eventSource), maxTickSkew)
        }
      case Some(tickWatermark) =>
        eventSource.maxTick().flatMap {
          case Some(maxEventSourceTick) =>
            resume(eventSource, maxEventSourceTick)(selector(eventSource), tickWatermark, maxTickSkew)
          case None =>
            throw new IllegalStateException(s"Tick watermark is $tickWatermark, but event source is empty: $eventSource")
        }
    }
  }

  private def start(es: ES, maxEventSourceTickAtStart: Option[Long])(
      selector: es.Selector, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    val replayProcessingDone: Future[Option[ReplayResult]] = maxEventSourceTickAtStart match {
      case None => // EventSource is empty
        Future successful None
      case Some(_) =>
        val replayProc = StreamPromise(replayProcessor(es))
        es.query(selector)(replayProc)
        replayProc.future.map(Option(_))(Threads.PiggyBack)
    }
    replayProcessingDone.flatMap { replayResult =>
      val liveProcessor = this.liveProcessor(es, replayResult)
      val liveSubscription = es.subscribe(selector.toComplete)(liveProcessor)
      // close window of opportunity, for a potential race condition, by re-querying anything since start
      val windowClosed = maxEventSourceTickAtStart match {
        case None =>
          val windowQuery = es.query(selector) _
          StreamPromise.foreach(windowQuery)(liveProcessor)
        case Some(maxEventSourceTickAtStart) =>
          val windowQuery = es.querySince(maxEventSourceTickAtStart - maxTickSkew, selector) _
          StreamPromise.foreach(windowQuery)(liveProcessor)
      }
      windowClosed.map(_ => liveSubscription)
    }
  }
  private def resume(es: ES, maxEventSourceTickAtStart: Long)(
      selector: es.Selector, tickWatermark: Long, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    val replayProcessingDone: Future[Option[ReplayResult]] = {
      val replayProc = StreamPromise(replayProcessor(es))
      es.querySince(tickWatermark - maxTickSkew, selector)(replayProc)
      replayProc.future.map(Option(_))(Threads.PiggyBack)
    }
    replayProcessingDone.flatMap { replayResult =>
      val liveProcessor = this.liveProcessor(es, replayResult)
      val liveSubscription = es.subscribe(selector.toComplete)(liveProcessor)
      // close window of opportunity for potential race condition, by re-querying anything since start
      val windowQuery = es.querySince(maxEventSourceTickAtStart - maxTickSkew, selector) _
      StreamPromise
        .foreach(windowQuery)(liveProcessor)
        .map(_ => liveSubscription)
    }
  }

}

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
  * batch and real-time transaction processors, useful for
  * building read models and other transaction processors,
  * e.g. sagas.
  * Can be started and stopped on demand, without loss of
  * transactions.
  * @param maxTickProcessed The highest tick processed.
  * NOTE: Since batch processing is generally
  * not done in tick order, a tick received
  * through incomplete batch processing
  * is NOT reliable as a high-water mark. Any
  * tick received in real-time processing can
  * be considered a high-water mark, and so
  * can ticks from completed batch processing.
  */
trait EventSourceConsumer[ID, EVT] {

  protected type BatchResult

  type ES = EventSource[ID, _ >: EVT]
  type TXN = Transaction[ID, _ >: EVT]

  /** Transaction selector. */
  protected def selector(es: ES): es.Selector

  /**
    * Called at startup, when batch processing of
    * transactions begins. Unlike real-time processing,
    * there will be no duplicate revisions passed
    * to batch processor.
    *
    * Batch processing allows
    * for much faster in-memory processing, because
    * persistence can be delayed until done.
    *
    * NOTE: Tick order is arbitrary, thus no guarantee
    * of causal tick processing, between different streams,
    * although, as mentioned, individual streams will be
    * processed in causal (monotonic revision) order.
    * If the event source is empty, this method will
    * not be called.
    *
    * It is highly recommended to return an instance of
    * [[delta.util.MonotonicBatchProcessor]] here.
    */
  protected def batchProcessor(es: ES): StreamConsumer[TXN, Future[BatchResult]]

  /**
    * When batch processing is completed, a real-time processor
    * is requested.
    * A real-time processor must take care to handle these
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
    * @param batchResult The result of batch processing, if any.
    */
  protected def realtimeProcessor(es: ES, batchResult: Option[BatchResult]): TXN => _

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
    val batchProcessingDone: Future[Option[BatchResult]] = maxEventSourceTickAtStart match {
      case None => // EventSource is empty
        Future successful None
      case Some(_) =>
        val batchProc = StreamPromise(batchProcessor(es))
        es.query(selector)(batchProc)
        batchProc.future.map(Option(_))(Threads.PiggyBack)
    }
    batchProcessingDone.flatMap { batchResult =>
      val realtimeProcessor = this.realtimeProcessor(es, batchResult)
      val liveSubscription = es.subscribe(selector.toComplete)(realtimeProcessor)
      // close window of opportunity, for a potential race condition, by re-querying anything since start
      val windowClosed = maxEventSourceTickAtStart match {
        case None =>
          val windowQuery = es.query(selector) _
          StreamPromise.foreach(windowQuery)(realtimeProcessor)
        case Some(maxEventSourceTickAtStart) =>
          val windowQuery = es.querySince(maxEventSourceTickAtStart - maxTickSkew, selector) _
          StreamPromise.foreach(windowQuery)(realtimeProcessor)
      }
      windowClosed.map(_ => liveSubscription)
    }
  }
  private def resume(es: ES, maxEventSourceTickAtStart: Long)(
      selector: es.Selector, tickWatermark: Long, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    val batchProcessingDone: Future[Option[BatchResult]] = {
      val batchProc = StreamPromise(batchProcessor(es))
      es.querySince(tickWatermark - maxTickSkew, selector)(batchProc)
      batchProc.future.map(Option(_))(Threads.PiggyBack)
    }
    batchProcessingDone.flatMap { batchResult =>
      val realtimeProcessor = this.realtimeProcessor(es, batchResult)
      val liveSubscription = es.subscribe(selector.toComplete)(realtimeProcessor)
      // close window of opportunity, for a potential race condition, by re-querying anything since start
      val windowQuery = es.querySince(maxEventSourceTickAtStart - maxTickSkew, selector) _
      StreamPromise
        .foreach(windowQuery)(realtimeProcessor)
        .map(_ => liveSubscription)
    }
  }

}

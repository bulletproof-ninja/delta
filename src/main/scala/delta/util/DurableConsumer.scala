package delta.util

import scala.concurrent.{ ExecutionContext, Future }

import delta.EventSource
import scuff.Subscription
import scuff.concurrent.StreamPromise
import scuff.concurrent.StreamConsumer
import scuff.concurrent.Threads

/**
  * A durable consumer of transactions,
  * useful for building read models and
  * other stateful processors.
  * Can be started and stopped on demand,
  * without loss of transactions.
  */
trait DurableConsumer[ID, EVT, CH] {
  type BatchResult

  type ES = EventSource[ID, EVT, CH]
  type TXN = ES#TXN

  /** Transaction selector. */
  protected def selector[T <: ES](es: T): es.Selector
  /**
    * The latest tick processed.
    * NOTE: Since batch processing is generally
    * not done in tick order, a tick received
    * through incomplete batch processing
    * is NOT reliable as a high-water mark. Any
    * tick received in real-time processing can
    * be considered a high-water mark, and so
    * can ticks from completed batch processing.
    */
  protected def maxTick: Future[Option[Long]]

  /**
    * Called when historic batch processing begins.
    * This is distinct from real-time processing and
    * allows the following optimizations:
    *     1) Predictable monotonic increasing revisions
    *     2) No duplicate revisions
    *     3) In-memory processing
    * NOTE: Tick order is arbitrary, thus no guarantee
    * of causal tick processing, between different streams,
    * although, as mentioned, individual streams will be
    * processed in causal (monotonic revision) order.
    * If the event source is empty, this will
    * not be called.
    */
  protected def batchProcessor[T <: ES](es: T): BatchProcessor
  type BatchProcessor = StreamConsumer[TXN, BatchResult]

  /**
    * When batch processing is completed, a real-time processor
    * is requested.
    * A real-time processor must take care to handle these
    * conditions:
    *     1) Out-of-order revisions
    *     2) Duplicate revisions
    *     3) Concurrent calls (depending on pub/sub implementation)
    * Using the [[MonotonicProcessor]] trait will handle all 3.
    * @param es The [[EventSource]] being processed.
    * @param batchResult The result of batch processing, if any.
    * `None` indicates that no transactions exists, which will only happen
    * when starting from scratch.
    */
  protected def realtimeProcessor[T <: ES](es: T, batchResult: Option[BatchResult]): RealtimeProcessor
  type RealtimeProcessor = (TXN => Any)

  /**
    * Start processing of transactions, either from beginning
    * of time, or from the tick watermark.
    * @param eventSource The [[EventSource]] to process.
    * @param maxTickSkew The max tick skew. Larger is safer, but be reasonable.
    * @return A future subscription to live events.
    * This will be available when historic processing is done,
    * thus state is considered current.
    */
  def process(eventSource: ES, maxTickSkew: Int)(implicit ec: ExecutionContext): Future[Subscription] = {
    require(maxTickSkew >= 0, s"Cannot have negative tick skew: $maxTickSkew")
    this.maxTick.flatMap {
      case None =>
        eventSource.maxTick().flatMap { maxExistingTick =>
          start(eventSource, maxExistingTick)(selector(eventSource), maxTickSkew)
        }
      case Some(maxProccessedTick) =>
        eventSource.maxTick().flatMap {
          case Some(maxExistingTick) =>
            resume(eventSource, maxExistingTick)(selector(eventSource), maxProccessedTick, maxTickSkew)
          case None =>
            throw new IllegalStateException(s"Highest tick is $maxProccessedTick, but event source is empty: $eventSource")
        }
    }
  }

  private def start(es: ES, maxProcessedTickAtStart: Option[Long])(
      selector: es.Selector, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    val batchProcessingDone: Future[Option[BatchResult]] = maxProcessedTickAtStart match {
      case None => // EventSource is empty
        Future successful None
      case Some(_) =>
        val batchProc = StreamPromise(batchProcessor(es))
        es.query(selector)(batchProc)
        batchProc.future.map(Option(_))(Threads.PiggyBack)
    }
    batchProcessingDone.flatMap { batchResult =>
      val realtimeProcessor = this.realtimeProcessor(es, batchResult)
      val liveSubscription = es.subscribe(selector.toMonotonic)(realtimeProcessor)
      // close window of opportunity, for a potential race condition, by re-querying anything since start
      val windowClosed = maxProcessedTickAtStart match {
        case None =>
          val windowQuery = es.query(selector) _
          StreamPromise.foreach(windowQuery)(realtimeProcessor)
        case Some(maxProcessedTickAtStart) =>
          val windowQuery = es.querySince(maxProcessedTickAtStart - maxTickSkew, selector) _
          StreamPromise.foreach(windowQuery)(realtimeProcessor)
      }
      windowClosed.map(_ => liveSubscription)
    }
  }
  private def resume(es: ES, maxProcessedTickAtStart: Long)(
      selector: es.Selector, maxProccessedTick: Long, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    val batchProcessingDone: Future[Option[BatchResult]] = {
      val batchProc = StreamPromise(batchProcessor(es))
      es.querySince(maxProccessedTick - maxTickSkew, selector)(batchProc)
      batchProc.future.map(Option(_))(Threads.PiggyBack)
    }
    batchProcessingDone.flatMap { batchResult =>
      val realtimeProcessor = this.realtimeProcessor(es, batchResult)
      val liveSubscription = es.subscribe(selector.toMonotonic)(realtimeProcessor)
      // close window of opportunity, for a potential race condition, by re-querying anything since start
      val windowQuery = es.querySince(maxProcessedTickAtStart - maxTickSkew, selector) _
      StreamPromise
        .foreach(windowQuery)(realtimeProcessor)
        .map(_ => liveSubscription)
    }
  }

}

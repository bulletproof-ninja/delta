package delta.util

import scala.concurrent.{ ExecutionContext, Future }

import delta.{ EventSource, Transaction }
import scuff.Subscription
import scuff.concurrent.StreamPromise

/**
  * A durable consumer of transactions,
  * useful for building read models and
  * other stateful processors.
  * Can be started and stopped on demand,
  * without loss of transactions.
  */
trait DurableConsumer[ID, EVT, CH] {

  type TXN = Transaction[ID, EVT, CH]
  type ES = EventSource[ID, EVT, CH]

  /** Transaction selector. */
  protected def selector[T <: ES](es: T): es.Selector
  /**
    * The highest real-time tick processed.
    * NOTE: Since historic processing is not
    * done in tick order, a tick received
    * through incomplete historic processing
    * is NOT reliable as a high-water mark. Any
    * tick received in real-time processing can
    * be considered a high-water mark.
    */
  protected def maxRealTimeProccessedTick: Future[Option[Long]]
  /**
    * Called when historic processing begins.
    * This is distinct from real-time processing and
    * allows the following optimizations:
    *     1) Predictable monotonic increasing revisions
    *     2) No duplicate revisions
    *     3) Potential for in-memory processing
    * NOTE: Tick order is arbitrary, thus no guarantee
    * of causal tick processing, between different streams,
    * although, as mentioned, individual streams will be
    * processed in causal (monotonic revision) order.
    * If the event source is empty, this will
    * not be called.
    */
  protected def batchProcessor[T <: ES](es: T): TXN => Unit
  /**
    * When historic batch processing is completed,
    * a real-time processor is requested.
    * A real-time processor must take care to handle these
    * conditions:
    *     1) Out-of-order revisions
    *     2) Duplicate transactions
    *     3) Concurrent calls (depending on pub/sub implementation)
    * For condition 3, the [[SerialAsyncProcessing]] class
    * can be used to serialize processing.
    */
  protected def realTimeProcessor[T <: ES](es: T): Future[TXN => Unit]

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
    this.maxRealTimeProccessedTick.flatMap {
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

  private def start(es: ES, maxExistingTickAtStart: Option[Long])(
    selector: es.Selector, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    val batchProcessingDone: Future[Unit] = maxExistingTickAtStart match {
      case None => // EventSource is empty
        Future successful None
      case Some(_) =>
        val batchQuery = es.query(selector) _
        StreamPromise.foreach(batchQuery)(batchProcessor(es))
    }
    batchProcessingDone.flatMap { _ =>
      this.realTimeProcessor(es).flatMap { realTimeProcessor =>
        val liveSubscription = es.subscribe(selector.toMonotonic)(realTimeProcessor)
        // close window of opportunity, for a potential race condition, by re-querying anything since start
        val windowClosed = maxExistingTickAtStart match {
          case None =>
            val windowQuery = es.query(selector) _
            StreamPromise.foreach(windowQuery)(realTimeProcessor)
          case Some(maxExistingTickAtStart) =>
            val windowQuery = es.querySince(maxExistingTickAtStart - maxTickSkew, selector) _
            StreamPromise.foreach(windowQuery)(realTimeProcessor)
        }
        windowClosed.map(_ => liveSubscription)
      }
    }
  }
  private def resume(es: ES, maxExistingTickAtStart: Long)(
    selector: es.Selector, maxProccessedTick: Long, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    val historicProcessingDone: Future[Unit] = {
      val historicQuery = es.querySince(maxProccessedTick - maxTickSkew, selector) _
      StreamPromise.foreach(historicQuery)(batchProcessor(es))
    }
    historicProcessingDone.flatMap { _ =>
      this.realTimeProcessor(es).flatMap { realTimeProcessor =>
        val liveSubscription = es.subscribe(selector.toMonotonic)(realTimeProcessor)
        // close window of opportunity, for a potential race condition, by re-querying anything since start
        val windowQuery = es.querySince(maxExistingTickAtStart - maxTickSkew, selector) _
        StreamPromise
          .foreach(windowQuery)(realTimeProcessor)
          .map(_ => liveSubscription)
      }
    }
  }

}

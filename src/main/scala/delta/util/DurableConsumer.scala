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
   * The highest reliable tick processed.
   * NOTE: Since historic processing is not
   * done in tick order, a tick received
   * through incomplete historic processing
   * is NOT reliable as a water mark. Any
   * tick received in live processing can
   * be considered a watermark.
   */
  protected def tickWatermark: Future[Option[Long]]
  /**
    * Called when historic processing begins.
    * This is distinct from live processing and
    * allows the following optimizations:
    *     1) Predictable monotonic increasing revisions
    *     2) No duplicate revisions
    *     3) Potential for in-memory processing
    * NOTE: If the event source is empty, this will
    * not be called.
    */
  protected def getHistoricProcessor[T <: ES](es: T): TXN => Unit
  /**
    * When caught up on historic transactions,
    * a live processor is requested. Any in-memory
    * processing from historic processing can be
    * persisted when called, before returning the
    * processing function.
    * A live processor must take care to handle these
    * conditions:
    *     1) Out-of-order revisions
    *     2) Duplicate (repeated) revisions
    *     3) Concurrent calls
    * For condition 3, the [[SerialAsyncProcessing]] class
    * can be used to serialize processing.
    */
  protected def getLiveProcessor[T <: ES](es: T): Future[TXN => Unit]

  /**
    * Start processing of transactions, either from beginning
    * of time, or from the tick watermark.
    * @param eventSource The [[EventSource]] to process.
    * @param maxTickSkew The max tick skew. Larger is safer, but be reasonable.
    * @return A future subscription to live events.
    * This will be available when historic processing is done,
    * thus state is considered current.
    */
  def startProcessing(eventSource: ES, maxTickSkew: Int)(implicit ec: ExecutionContext): Future[Subscription] = {
    require(maxTickSkew >= 0, s"Cannot have negative tick skew: $maxTickSkew")
    this.tickWatermark.flatMap {
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

  private def start(es: ES, lastTickAtStart: Option[Long])(
    selector: es.Selector, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    val historicProcessingDone: Future[Unit] = lastTickAtStart match {
      case None => // EventSource is empty
        Future successful None
      case Some(_) =>
        val historicQuery = es.query(selector) _
        val historicProcessor = this.getHistoricProcessor(es)
        StreamPromise.foreach(historicQuery)(historicProcessor)
    }
    historicProcessingDone.flatMap { _ =>
      this.getLiveProcessor(es).flatMap { liveProcessor =>
        val liveSubscription = es.subscribe(selector.toMonotonic)(liveProcessor)
        // close window of opportunity, for a potential race condition, by re-querying anything since start
        val windowClosed = lastTickAtStart match {
          case None =>
            val windowQuery = es.query(selector) _
            StreamPromise.foreach(windowQuery)(liveProcessor)
          case Some(lastTickAtStart) =>
            val windowQuery = es.querySince(lastTickAtStart - maxTickSkew, selector) _
            StreamPromise.foreach(windowQuery)(liveProcessor)
        }
        windowClosed.map(_ => liveSubscription)
      }
    }
  }
  private def resume(es: ES, lastTickAtStart: Long)(
    selector: es.Selector, lastProcessedTick: Long, maxTickSkew: Int)(
      implicit ec: ExecutionContext): Future[Subscription] = {
    val historicProcessingDone: Future[Unit] = {
      val historicQuery = es.querySince(lastProcessedTick - maxTickSkew, selector) _
      val historicProcessor = this.getHistoricProcessor(es)
      StreamPromise.foreach(historicQuery)(historicProcessor)
    }
    historicProcessingDone.flatMap { _ =>
      this.getLiveProcessor(es).flatMap { liveProcessor =>
        val liveSubscription = es.subscribe(selector.toMonotonic)(liveProcessor)
        // close window of opportunity, for a potential race condition, by re-querying anything since start
        val windowQuery = es.querySince(lastTickAtStart - maxTickSkew, selector) _
        StreamPromise
          .foreach(windowQuery)(liveProcessor)
          .map(_ => liveSubscription)
      }
    }
  }

}

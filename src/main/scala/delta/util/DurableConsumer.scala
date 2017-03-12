package delta.util

import scala.concurrent.{ ExecutionContext, Future }

import delta.{ EventSource, Transaction }
import scuff.Subscription
import scuff.concurrent.StreamPromise

trait DurableConsumer[ID, EVT, CH] {

  type TXN = Transaction[ID, EVT, CH]
  type ES = EventSource[ID, EVT, CH]

  /** Transaction selector. */
  protected def selector[T <: ES](es: T): es.Selector
  /** Last processed tick, if exists. */
  protected def lastProcessedTick: Future[Option[Long]]
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
  protected def getHistoricProcessor(): TXN => Unit
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
    */
  protected def getLiveProcessor(): Future[TXN => Unit]

  /**
    * Start processing of transactions, either from beginning
    * of time, or from where left off when last ended.
    * @param es The [[EventSource]] to process.
    * @param maxTickSkew The anticipated max tick skew.
    * @return A future subscription to live events.
    * This will be available when historic processing is done,
    * thus state is considered current.
    */
  def startProcessing(es: ES, maxTickSkew: Int)(implicit ec: ExecutionContext): Future[Subscription] = {
    require(maxTickSkew >= 0, s"Cannot have negative tick skew: $maxTickSkew")
    this.lastProcessedTick.flatMap {
      case None =>
        es.lastTick().flatMap { maybeLastTick =>
          start(es, maybeLastTick)(selector(es), maxTickSkew)
        }
      case Some(lastProcessed) =>
        es.lastTick().flatMap {
          case Some(lastTick) =>
            resume(es, lastTick)(selector(es), lastProcessed, maxTickSkew)
          case None =>
            throw new IllegalStateException(s"Last processed tick is $lastProcessed, but event source is empty: $es")
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
        val historicProcessor = this.getHistoricProcessor()
        StreamPromise.foreach(historicQuery)(historicProcessor)
    }
    historicProcessingDone.flatMap { _ =>
      this.getLiveProcessor().flatMap { liveProcessor =>
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
      val historicProcessor = this.getHistoricProcessor()
      StreamPromise.foreach(historicQuery)(historicProcessor)
    }
    historicProcessingDone.flatMap { _ =>
      this.getLiveProcessor().flatMap { liveProcessor =>
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

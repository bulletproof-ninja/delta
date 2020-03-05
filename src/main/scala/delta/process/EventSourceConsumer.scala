package delta.process

import scala.concurrent.{ ExecutionContext, Future }

import scuff.Subscription
import scuff.StreamConsumer
import scuff.concurrent.StreamPromise
import scuff.concurrent._

/**
 * A gap-less event source consumer, consisting of
 * replay and live transaction processors, useful for
 * building read models and other transaction processors,
 * e.g. sagas.
 */
trait EventSourceConsumer[ID, EVT] {

  protected type ReplayResult

  type EventSource = delta.EventSource[ID, _ >: EVT]
  protected type Transaction = delta.Transaction[ID, _ >: EVT]

  /**
   * The maximum tick skew, i.e. the largest reasonably
   * possible wall-clock skew combined with network
   * propagation delay, and in other factors that might
   * affect tick differences.
   */
  protected def maxTickSkew: Int

  /** Transaction selector. */
  protected def selector(es: EventSource): es.Selector

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
   * [[delta.process.MonotonicReplayProcessor]] here.
   */
  protected def replayProcessor(es: EventSource): StreamConsumer[Transaction, Future[ReplayResult]]

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
   * [[delta.process.MonotonicProcessor]] here,
   * which will handle all 3 above cases.
   *
   * @param es The [[delta.EventSource]] being processed.
   * @param replayResult The result of replay processing, if any.
   * @return A live transaction processing function
   */
  protected def liveProcessor(es: EventSource, replayResult: Option[ReplayResult]): Transaction => Any

  /** The currently processed tick watermark. */
  protected def tickWatermark: Option[Long]

  /**
   * Start consumption of transactions, either from the
   * very beginning or from the tick watermark.
   * @param eventSource The [[EventSource]] to process.
   * @return A future subscription to live events.
   * This will be available when replay processing is done,
   * thus state is considered current. Or `None` if publishiing
   * is not supported by event source.
   */
  def consume(eventSource: EventSource)(
      implicit
      ec: ExecutionContext): Future[Subscription] = {
    val maxTickSkew = this.maxTickSkew
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

  private def start(es: EventSource, maxEventSourceTickAtStart: Option[Long])(
      selector: es.Selector, maxTickSkew: Int)(
      implicit
      ec: ExecutionContext): Future[Subscription] = {
    val replayProcessingDone: Future[Option[ReplayResult]] = maxEventSourceTickAtStart match {
      case None => // EventSource is empty
        Future.none
      case Some(_) =>
        val replayProc = StreamPromise(replayProcessor(es))
        es.query(selector)(replayProc)
        replayProc.future.map(Option(_))(Threads.PiggyBack)
    }
    replayProcessingDone.flatMap { replayResult =>
      val liveProcessor = this.liveProcessor(es, replayResult)
      val liveSubscription = es.subscribe(selector.toStreamsSelector)(liveProcessor)
      // close window of opportunity for
      // potential race condition with subscription,
      // by re-querying anything since starting replay
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
  private def resume(es: EventSource, maxEventSourceTickAtStart: Long)(
      selector: es.Selector, tickWatermark: Long, maxTickSkew: Int)(
      implicit
      ec: ExecutionContext): Future[Subscription] = {
    val replayProcessingDone: Future[Option[ReplayResult]] = {
      val replayProc = StreamPromise(replayProcessor(es))
      es.querySince(tickWatermark - maxTickSkew, selector)(replayProc)
      replayProc.future.map(Option(_))(Threads.PiggyBack)
    }
    replayProcessingDone.flatMap { replayResult =>
      val liveProcessor = this.liveProcessor(es, replayResult)
      val liveSubscription = es.subscribe(selector.toStreamsSelector)(liveProcessor)
      // close window of opportunity for
      // potential race condition with subscription,
      // by re-querying anything since starting replay
      val windowQuery = es.querySince(maxEventSourceTickAtStart - maxTickSkew, selector) _
      StreamPromise
        .foreach(windowQuery)(liveProcessor)
        .map(_ => liveSubscription)
    }
  }

}

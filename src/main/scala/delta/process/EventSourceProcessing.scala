package delta.process

import scuff.StreamConsumer
import scuff.concurrent.StreamPromise

import scala.concurrent._

/**
  * General [[delta.EventSource]] processing trait.
  *
  * @see delta.process.PersistentMonotonicProcessing
  *
  * @tparam SID The stream id type
  * @tparam EVT The processing event type. Can be a sub-type of the evemt source event type
  */
trait EventSourceProcessing[SID, EVT] {

  protected type LiveResult

  type Tick = delta.Tick
  type Revision = delta.Revision
  type EventSource = delta.EventSource[SID, _ >: EVT]
  protected type Transaction = delta.Transaction[SID, _ >: EVT]

  protected type ReplayProcessor = StreamConsumer[Transaction, Future[Unit]]
  protected type LiveProcessor = Transaction => Future[LiveResult]

  /**
   * The largest possible tick skew combined with network
   * propagation delay, and in other factors that might affect
   * tick differences.
   * @note A tick window that's _too large_ can lead to unnecessary
   * processing of already processed transactions. But it won't
   * affect correctness. A tick window that's too small could
   * possibly miss processing of transactions, thus the known state
   * will be outdated. Any subsequent updates to stale state will
   * correct this, so it's an intermittent problem, but only if state
   * is updated again, which might not happen. In other words, unless
   * other reasons exist, it's safer to make the window larger than
   * smaller.
   */
  protected def tickWindow: Int

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
   * @note Tick order is arbitrary, thus no guarantee
   * of causal tick processing, between different streams,
   * although, as mentioned, individual streams will be
   * processed in causal (monotonic revision) order.
   * If the event source is empty, this method will
   * not be called.
   *
   * It is highly recommended to return an instance of
   * [[delta.process.MonotonicReplayProcessor]] here.
   */
  protected def replayProcessor(es: EventSource): ReplayProcessor

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
  protected def liveProcessor(es: EventSource): LiveProcessor

  /** The currently processed tick watermark. */
  protected def tickWatermark: Option[Tick]

  /**
   * Catch up on missed historic transactions, if any,
   * either from beginning of time, or from the tick watermark.
   * @param eventSource The [[EventSource]] to process.
   * @return A future subscription to live events.
   * This will be available when historic processing is done,
   * thus state is considered current.
   */
  protected def catchUp(
      eventSource: EventSource): Future[Unit] = {
    val tickWindow = this.tickWindow
    require(tickWindow >= 0, s"Cannot have negative tick window: $tickWindow")
    tickWatermark match {
      case Some(tickWatermark) =>
        resume(eventSource)(selector(eventSource), tickWatermark, tickWindow)
      case None =>
        begin(eventSource)(selector(eventSource))
    }
  }

  private def begin(es: EventSource)(
      selector: es.Selector): Future[Unit] = {
    val replayProc = StreamPromise(replayProcessor(es))
    es.query(selector)(replayProc)
    replayProc.future
  }
  private def resume(es: EventSource)(
      selector: es.Selector, tickWatermark: Tick, tickWindow: Int): Future[Unit] = {
    val replayProc = StreamPromise(replayProcessor(es))
    es.querySince(tickWatermark - tickWindow, selector)(replayProc)
    replayProc.future
  }

}

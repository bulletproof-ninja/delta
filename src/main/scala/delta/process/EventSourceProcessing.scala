package delta.process

import scuff.Reduction
import scuff.concurrent._

import scala.util._
import scala.concurrent._

import ReplayCompletion._
import scala.concurrent.duration.FiniteDuration
import scala.collection.concurrent.TrieMap
import scala.collection.immutable

/**
  * General [[delta.EventSource]] processing trait.
  *
  * @see delta.process.IdempotentProcessing
  *
  * @tparam SID The stream id type
  * @tparam EVT The processing event type. Can be a sub-type of the evemt source event type
  */
trait EventSourceProcessing[SID, EVT] {

  def name: String

  protected type LiveResult

  type Tick = delta.Tick
  type Revision = delta.Revision
  type EventSource = delta.EventSource[SID, _ >: EVT]
  protected type Transaction = delta.Transaction[SID, _ >: EVT]

  protected type ReplayProcessor =
    AsyncReduction[Transaction, ReplayCompletion[SID]]
    with ReplayStatus
  protected type LiveProcessor =
    Transaction => Future[LiveResult]

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
  protected def replayProcessor(
      es: EventSource,
      config: ReplayProcessConfig)
      : ReplayProcessor

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
   * @return A live transaction processing function
   */
  protected def liveProcessor(
      es: EventSource,
      config: LiveProcessConfig)
      : LiveProcessor

  /**
   * Catch up on missed historic transactions, if any,
   * either from beginning of time, or from the tick watermark.
   * @param eventSource The [[EventSource]] to process.
   * @param maxTick The latest tick written
   * @param eventSource The [[EventSource]] to process.
   * @return A future subscription to live events.
   * This will be available when historic processing is done,
   * thus state is considered current.
   */
  protected def catchUp(
      eventSource: EventSource,
      maxTick: Option[Tick],
      replayConfig: ReplayProcessConfig)
      : ReplayProcess[ReplayCompletion[SID]] = {
    Try(replayProcessor(eventSource, replayConfig)) match {
      case Failure(cause) =>
        ReplayProcess.failed(this.name, cause)
      case Success(replayProc) =>
        val replayFuture =
          replayConfig.adjustToWindow(maxTick) match {
            case None =>
              eventSource.query(selector(eventSource))(replayProc)
            case Some(fromTick) =>
              eventSource.querySince(fromTick, selector(eventSource))(replayProc)
          }
        ReplayProcess(replayProc, replayFuture)
    }
  }

  type StreamFailures = Map[SID, Throwable]

  /**
    * Replay transactions missed during replay
    * processing. However, since replay state is
    * now lost, we replay all transactions since
    * the first missing revision.
    * @return Stream failures, if any
    */
  protected def completeStreams(
      eventSource: EventSource,
      brokenStreams: List[BrokenStream[SID]],
      processor: LiveProcessor,
      completionTimeout: FiniteDuration)(
      implicit
      ec: ExecutionContext)
      : Future[StreamFailures] =

    if (brokenStreams.isEmpty) Future successful Map.empty
    else {
      type Transaction = eventSource.Transaction
      val streamErrors = new TrieMap[SID, Throwable]
      val replayResults: Iterator[Future[Unit]] =
        brokenStreams.iterator
          .map {
            case ProcessingFailure(streamId, _, cause) =>
              streamErrors.putIfAbsent(streamId, cause)
              Future.unit
            case MissingRevisions(streamId, _, missingRevisions) =>
              eventSource.replayStreamFrom(streamId, missingRevisions.head) {
                new AsyncReduction[Transaction, Unit] {
                  def resultTimeout = completionTimeout
                  def asyncNext(tx: Transaction) = processor(tx)
                  def asyncResult(
                      timeout: Option[TimeoutException],
                      processErrors: List[(Transaction, Throwable)]) = {
                    processErrors.foreach {
                      case (tx, cause) =>
                        streamErrors.putIfAbsent(tx.stream, cause)
                    }
                    timeout.foreach {
                      streamErrors.putIfAbsent(streamId, _)
                    }
                    Future.unit
                  }
                }
            }
          }

      Future.sequence(replayResults)
        .map { _ => streamErrors.toMap }

    }

}

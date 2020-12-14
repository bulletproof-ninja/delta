package delta.process

import scuff.StreamConsumer
import scuff.concurrent.StreamPromise

import scala.util._
import scala.concurrent._

import ReplayCompletion.IncompleteStream

/**
  * General [[delta.EventSource]] processing trait.
  *
  * @see delta.process.PersistentMonotonicProcessing
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

  protected type ReplayProcessor = StreamConsumer[Transaction, Future[ReplayCompletion[SID]]] with ReplayStatus
  protected type LiveProcessor = Transaction => Future[LiveResult]

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
      eventSource: EventSource,
      replayConfig: ReplayProcessConfig)
      : ReplayProcess[ReplayCompletion[SID]] = {
    Try(replayProcessor(eventSource, replayConfig)) match {
      case Failure(cause) =>
        ReplayProcess.failed(this.name, cause)
      case Success(replayProc) =>
        val replayPromise = StreamPromise(replayProc)
        replayConfig.adjustToWindow(tickWatermark) match {
          case None =>
            eventSource.query(selector(eventSource))(replayPromise)
          case Some(fromTick) =>
            eventSource.querySince(fromTick, selector(eventSource))(replayPromise)
        }
        ReplayProcess(replayProc,  replayPromise.future)
    }
  }

  /**
    * Replay transactions missed during replay
    * processing. However, since replay state is
    * now lost, we replay all transactions since
    * the first missing revision.
    * @return Stream failures, if any
    */
  protected def completeStreams(
      eventSource: EventSource,
      incompleteStreams: List[ReplayCompletion.IncompleteStream[SID]],
      processor: LiveProcessor)(
      implicit
      ec: ExecutionContext): Future[Map[SID, Throwable]] =

    if (incompleteStreams.isEmpty) Future successful Map.empty
    else {
      val replayer = new MissingRevisionsReplayer(eventSource)
      val replays =
        incompleteStreams.iterator
          .flatMap {
            case IncompleteStream(id, Success(missingRevisions)) =>
              id -> replayer.requestReplayFrom(id, missingRevisions.head)(processor) :: Nil
            case IncompleteStream(id, Failure(cause)) =>
              id -> (Future failed cause) :: Nil
            case _ =>
              Nil
          }
      Future.sequence(replays.map(_._2)).map { _ =>
          replays.flatMap {
            case (id, future) =>
              future.value.flatMap {
                case Failure(cause) => Some(id -> cause)
                case _ => None
              }
          }.toMap
        }
    }

}

package delta.validation

import delta.process._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/**
  * Process for validation of [[delta.EventStore]].
  * @note Before activating the validating event store
  * (through `eventStore.activate(thisProcess)`), the
  * event store should be validated first, to ensure no
  * invalid state exists, which might happen during a
  * unplanned shutdown.
  */
trait EventStoreValidationProcess[SID, EVT, S]
extends EventSourceProcessing[SID, EVT] {

  protected type Channel = delta.Channel

  protected override type LiveResult <: S

  /** Lookup compensation function for channel. */
  val compensation: PartialFunction[Channel, Compensation[SID, S]]

  type ConsistentEventStore = EventSource with ConsistencyValidation[SID, _ >: EVT]

  /** The transaction proceesor, per instance. */
  def txProcessor(es: EventSource, config: LiveProcessConfig): LiveProcessor =
    this.liveProcessor(es, config)

  /**
    * Validate any outstanding transactions.
    * @note Should only be done from a single instance
    * at startup.
    */
  def validate(
      eventStore: ConsistentEventStore,
      replayConfig: ReplayProcessConfig)
      : ReplayProcess[ReplayCompletion[SID]] =
    this.catchUp(eventStore: EventSource, replayConfig)

  override def completeStreams(
      eventSource: EventSource,
      incompleteStreams: List[ReplayCompletion.IncompleteStream[SID]],
      processor: LiveProcessor)(
      implicit
      ec: ExecutionContext)
      : Future[Map[SID, Throwable]] =
    super.completeStreams(eventSource, incompleteStreams, processor)
}

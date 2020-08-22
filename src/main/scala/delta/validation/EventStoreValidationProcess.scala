package delta.validation

import delta.process.EventSourceProcessing
import scala.concurrent.Future

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
  def txProcessor(es: EventSource): LiveProcessor =
    this.liveProcessor(es)

  /**
    * Validate any outstanding transactions.
    * @note Should only be done from a single instance.
    */
  def validate(
      eventStore: ConsistentEventStore)
      : Future[Unit] =
    this.catchUp(eventStore: EventSource)

}

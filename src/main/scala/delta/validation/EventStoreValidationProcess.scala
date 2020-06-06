package delta.validation

import delta.process.EventSourceProcessing
import scala.concurrent.Future

trait EventStoreValidationProcess[SID, EVT, S]
extends EventSourceProcessing[SID, EVT] {

  protected type Channel = delta.Channel

  protected override type LiveResult <: S

  /** Lookup compensation function for channel. */
  def compensation(ch: Channel): Option[Compensation[SID, S]]

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
    super.catchUp(eventStore: EventSource)

}

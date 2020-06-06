package delta

import scala.concurrent.Future
import scala.util.control.NoStackTrace

/**
 * Event store. Extension of [[delta.EventSource]] that
 * allows committing event transactions.
 * @tparam ID Stream id type
 * @tparam EVT Event type
 */
trait EventStore[ID, EVT]
  extends EventSource[ID, EVT] {

  protected def Transaction(
      tick: Tick,
      channel: Channel,
      stream: ID,
      revision: Revision,
      metadata: Map[String, String],
      events: List[EVT]) =
    new Transaction(tick, channel, stream, revision, metadata, events)

  final class DuplicateRevisionException(val conflict: Transaction)
    extends RuntimeException(s"Revision ${conflict.revision} already exists for: ${conflict.stream}")
    with NoStackTrace {
    override def toString = super[RuntimeException].toString()
  }

  /**
   * Commit transaction.
   * @param stream Stream identifier.
   * @param revision Stream revision.
   * @param tick The clock tick
   * @param events The events, at least one.
   * @param metadata Optional metadata
   * @return Transaction, or if failed a possible
   * [[DuplicateRevisionException]] if the revision already exists.
   */
  def commit(channel: Channel, stream: ID, revision: Revision, tick: Tick,
      events: List[EVT], metadata: Map[String, String] = Map.empty): Future[Transaction]

}

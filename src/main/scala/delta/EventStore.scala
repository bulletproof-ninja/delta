package delta

import scala.concurrent.Future
import scala.util.control.NoStackTrace

/**
  * Event store.
  * @tparam ID Id type
  * @tparam EVT Event type
  * @tparam CH Channel type
  */
trait EventStore[ID, EVT]
    extends EventSource[ID, EVT] {

  protected def Transaction(
    tick: Long,
    channel: String,
    stream: ID,
    revision: Int,
    metadata: Map[String, String],
    events: List[EVT]) = new TXN(tick, channel, stream, revision, metadata, events)

  final case class DuplicateRevisionException(conflict: TXN)
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
  def commit(channel: String, stream: ID, revision: Int, tick: Long,
    events: List[EVT], metadata: Map[String, String] = Map.empty): Future[TXN]

}

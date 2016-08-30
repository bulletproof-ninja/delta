package ulysses

import java.io.InvalidObjectException

import scala.collection.{ Seq => aSeq, Map => aMap }
import scala.concurrent.Future
import scala.util.control.NoStackTrace

import scuff.concurrent.StreamCallback

/**
  * Event store.
  * @tparam ID Id type
  * @tparam EVT Event type
  * @tparam CH Channel type
  */
trait EventStore[ID, EVT, CH]
    extends EventSource[ID, EVT, CH] {

  protected def Transaction(
    tick: Long,
    channel: CH,
    stream: ID,
    revision: Int,
    metadata: aMap[String, String],
    events: aSeq[EVT]) = new TXN(tick, channel, stream, revision, metadata.toMap, events.toVector)

  final case class DuplicateRevisionException(conflictingTransaction: TXN)
      extends RuntimeException(s"Revision ${conflictingTransaction.revision} already exists for: ${conflictingTransaction.stream}")
      with NoStackTrace {
    override def toString = super[RuntimeException].toString()
  }

  /**
    * Record transaction.
    * @param channel Stream channel.
    * @param stream Stream identifier.
    * @param revision Stream revision.
    * @param tick The clock tick
    * @param events The events, at least one.
    * @param metadata Optional metadata
    * @return Transaction, or if failed a possible
    * [[DuplicateRevisionException]] if the revision already exists.
    */
  def record(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: aSeq[EVT], metadata: aMap[String, String] = Map.empty): Future[TXN]

}

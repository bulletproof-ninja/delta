package ulysses

import scala.concurrent.Future
import scuff.concurrent.StreamCallback
import scuff.Subscription
import scala.concurrent.ExecutionContext

/**
  * Event source.
  */
trait EventSource[ID, EVT, CH] {

  type TXN = Transaction[ID, EVT, CH]

  def currRevision(stream: ID): Future[Option[Int]]
  def lastTick(): Future[Option[Long]]

  /** Replay complete stream. */
  def replayStream(stream: ID)(callback: StreamCallback[TXN]): Unit
  /** Replay stream from provided revision range. */
  def replayStreamRange(stream: ID, revisionRange: Range)(callback: StreamCallback[TXN]): Unit
  /** Replay stream from provided revision to current. */
  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[TXN]): Unit
  /** Replay stream from revision 0 to specific revision (inclusive). */
  def replayStreamTo(stream: ID, toRevision: Int)(callback: StreamCallback[TXN]): Unit =
    replayStreamRange(stream, 0 to toRevision)(callback)

  /** Query across streams, optionally providing a selector to filter results. */
  def query(selector: Selector = Everything)(callback: StreamCallback[TXN]): Unit
  /** Query across streams, optionally providing a selector to filter results. */
  def querySince(sinceTick: Long, selector: Selector = Everything)(callback: StreamCallback[TXN]): Unit

  def subscribe(
    selector: Selector = Everything)(
      callback: TXN => Unit): Subscription

  sealed abstract class Selector {
    def include(txn: TXN): Boolean
  }
  case object Everything extends Selector {
    def include(txn: TXN) = true
  }
  case class ChannelSelector(channels: Set[CH]) extends Selector {
    def this(one: CH, others: CH*) = this((one +: others).toSet)
    require(channels.nonEmpty)
    def include(txn: TXN) = channels.contains(txn.channel)
  }
  case class EventSelector(byChannel: Map[CH, Set[C[EVT]]])
      extends Selector {
    def this(chEvt: (CH, Set[C[EVT]]), more: (CH, Set[C[EVT]])*) =
      this(Map((chEvt :: more.toList): _*))
    require(byChannel.nonEmpty)
    require(byChannel.forall(_._2.nonEmpty), s"No events: ${byChannel.filter(_._2.isEmpty).map(_._1).mkString(", ")}")
    def include(txn: TXN) = byChannel.get(txn.channel).exists { set =>
      txn.events.iterator.map(_.getClass).exists(set.contains)
    }
  }
  case class StreamSelector(stream: ID, channel: CH) extends Selector {
    def include(txn: TXN) = txn.stream == stream && txn.channel == channel
  }

  type C[EVT] = Class[_ <: EVT]

  object Selector {
    def apply(ch: CH, one: C[EVT], others: C[EVT]*) =
      new EventSelector(Map(ch -> (one +: others).toSet))
    def apply(one: CH, others: CH*) =
      new ChannelSelector(one, others: _*)
    def apply(stream: ID, channel: CH) =
      new StreamSelector(stream, channel)
  }

}

package delta

import scala.concurrent.Future

import scuff.Subscription
import scuff.concurrent.StreamCallback

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

  /** Subscribe to published transactions. */
  def subscribe(
    selector: MonotonicSelector = Everything)(
      callback: TXN => Unit): Subscription

  type CEVT = Class[_ <: EVT]

  sealed abstract class Selector {
    def include(txn: TXN): Boolean
    def toMonotonic: EventSource.this.MonotonicSelector
  }
  sealed abstract class MonotonicSelector extends Selector
  case object Everything extends MonotonicSelector {
    def include(txn: TXN) = true
    def toMonotonic: MonotonicSelector = this
  }
  case class ChannelSelector(channels: Set[CH]) extends MonotonicSelector {
    def this(one: CH, others: CH*) = this((one +: others).toSet)
    require(channels.nonEmpty)
    def include(txn: TXN) = channels.contains(txn.channel)
    def toMonotonic: MonotonicSelector = this
  }
  case class EventSelector(byChannel: Map[CH, Set[CEVT]])
      extends Selector {
    def this(chEvt: (CH, Set[CEVT]), more: (CH, Set[CEVT])*) =
      this(Map((chEvt :: more.toList): _*))
    require(byChannel.nonEmpty)
    require(byChannel.forall(_._2.nonEmpty), s"No events: ${byChannel.filter(_._2.isEmpty).map(_._1).mkString(", ")}")
    def include(txn: TXN) = byChannel.get(txn.channel).exists { set =>
      txn.events.iterator.map(_.getClass).exists(set.contains)
    }
    def toMonotonic: MonotonicSelector = ChannelSelector(byChannel.keySet)
  }
  case class StreamSelector(stream: ID, channel: CH) extends MonotonicSelector {
    def include(txn: TXN) = txn.stream == stream && txn.channel == channel
    def toMonotonic: MonotonicSelector = this
  }

  object Selector {
    def apply(ch: CH, one: CEVT, others: CEVT*) =
      new EventSelector(Map(ch -> (one +: others).toSet))
    def apply(one: CH, others: CH*) =
      new ChannelSelector(one, others: _*)
    def apply(stream: ID, channel: CH) =
      new StreamSelector(stream, channel)
  }

}

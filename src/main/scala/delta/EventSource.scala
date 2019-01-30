package delta

import scala.concurrent.Future

import scuff.Subscription
import scuff.StreamConsumer
import scala.annotation.varargs
import scala.reflect.ClassTag

/**
  * Event source.
  */
trait EventSource[ID, EVT] {

  protected implicit val channelTag: ClassTag[Channel] = ClassTag(classOf[String])
  type Channel = Transaction.Channel
  type TXN = Transaction[ID, EVT]

  def currRevision(stream: ID): Future[Option[Int]]
  def maxTick(): Future[Option[Long]]

  protected[delta] type StreamReplayConsumer[E >: EVT, U] = StreamConsumer[Transaction[ID, E], U]

  /** Replay complete stream. */
  def replayStream[E >: EVT, U](stream: ID)(callback: StreamReplayConsumer[E, U]): Unit
  /** Replay stream from provided revision range. */
  def replayStreamRange[E >: EVT, U](stream: ID, revisionRange: Range)(callback: StreamReplayConsumer[E, U]): Unit
  /** Replay stream from provided revision to current. */
  def replayStreamFrom[E >: EVT, U](stream: ID, fromRevision: Int)(callback: StreamReplayConsumer[E, U]): Unit
  /** Replay stream from revision 0 to specific revision (inclusive). */
  def replayStreamTo[E >: EVT, U](stream: ID, toRevision: Int)(callback: StreamReplayConsumer[E, U]): Unit =
    replayStreamRange(stream, 0 to toRevision)(callback)

  /** Query across streams, optionally providing a selector to filter results. */
  def query[U](selector: Selector = Everything)(callback: StreamConsumer[TXN, U]): Unit
  /** Query across streams, optionally providing a selector to filter results. */
  def querySince[U](sinceTick: Long, selector: Selector = Everything)(callback: StreamConsumer[TXN, U]): Unit

  /** Subscribe to all transactions. */
  final def subscribe[U]()(callback: TXN => U): Subscription = subscribe(Everything)(callback)
  /** Subscribe to selected transactions. */
  def subscribe[U](
      selector: CompleteSelector)(
      callback: TXN => U): Subscription = sys.error("Publishing not enabled!")

  type CEVT = Class[_ <: EVT]

  /** General selector. */
  sealed abstract class Selector {
    def include(txn: TXN): Boolean
    def toComplete: EventSource.this.CompleteSelector
  }
  /** Complete, unbroken stream, selector. */
  sealed abstract class CompleteSelector extends Selector {
    /** Subset of channels, if any. */
    def channelSubset: Set[Channel]
  }

  /** All streams, all channels, unbroken. */
  case object Everything extends CompleteSelector {
    def channelSubset: Set[Channel] = Set.empty
    def include(txn: TXN) = true
    def toComplete: CompleteSelector = this
  }
  def ChannelSelector(one: Channel, others: Channel*): CompleteSelector =
    new ChannelSelector((one +: others).toSet)
  def ChannelSelector(channels: Seq[Channel]): CompleteSelector =
    new ChannelSelector(channels.toSet)
  /** All streams in the provided channels, unbroken. */
  case class ChannelSelector private[EventSource] (channels: Set[Channel]) extends CompleteSelector {
    //    def this(one: String, others: String*) = this((one +: others).toSet)
    require(channels.nonEmpty)
    def channelSubset: Set[Channel] = channels
    def include(txn: TXN) = channels.contains(txn.channel)
    def toComplete: CompleteSelector = this
  }
  def EventSelector(chEvt: (Channel, Set[CEVT]), more: (Channel, Set[CEVT])*): Selector =
    new EventSelector(Map((chEvt :: more.toList): _*))
  def EventSelector(byChannel: java.util.Map[Channel, java.util.Set[CEVT]]): Selector = {
    import collection.JavaConverters._
    new EventSelector(byChannel.asScala.toMap.mapValues(_.asScala.toSet).toMap)
  }
  /**
    * Only transactions containing the provided events.
    * This means broken, incomplete, streams. Should only
    * be used if necessary, i.e. on data sets so large that
    * it would otherwise take too long to process.
    * NOTE: Steps should be taken to ensure that the stream
    * revision number is still current, since this is no
    * longer guaranteed.
    */
  case class EventSelector private[EventSource] (byChannel: Map[Channel, Set[CEVT]])
    extends Selector {
    require(byChannel.nonEmpty)
    require(byChannel.forall(_._2.nonEmpty), s"No events: ${byChannel.filter(_._2.isEmpty).map(_._1).mkString(", ")}")
    def channelSubset = byChannel.keySet
    def include(txn: TXN) = byChannel.get(txn.channel).exists { set =>
      txn.events.exists { evt =>
        set.exists { cls =>
          cls isAssignableFrom evt.getClass
        }
      }
    }
    def toComplete: CompleteSelector = ChannelSelector(byChannel.keySet)
  }

  def StreamSelector(stream: ID, channel: Channel): CompleteSelector = new StreamSelector(stream, channel)
  case class StreamSelector private[EventSource] (stream: ID, channel: Channel) extends CompleteSelector {
    def channelSubset: Set[Channel] = Set(channel)
    def include(txn: TXN) = txn.stream == stream && txn.channel == channel
    def toComplete: CompleteSelector = this
  }

  object Selector {
    def Everything: CompleteSelector = EventSource.this.Everything
    @varargs
    def apply(ch: Channel, one: CEVT, others: CEVT*): Selector =
      new EventSelector(Map(ch -> (one +: others).toSet))
    @varargs
    def apply(one: Channel, more: Channel*): CompleteSelector =
      ChannelSelector(one, more: _*)
  }

}

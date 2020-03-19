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
  type Transaction = delta.Transaction[ID, EVT]

  def currRevision(stream: ID): Future[Option[Int]]
  def maxTick(): Future[Option[Long]]
  def ticker: Ticker

  /** Replay complete stream. */
  def replayStream[R](stream: ID)(callback: StreamConsumer[Transaction, R]): Unit
  /** Replay stream from provided revision range. */
  def replayStreamRange[R](stream: ID, revisionRange: Range)(callback: StreamConsumer[Transaction, R]): Unit
  /** Replay stream from provided revision to current. */
  def replayStreamFrom[R](stream: ID, fromRevision: Int)(callback: StreamConsumer[Transaction, R]): Unit
  /** Replay stream from revision 0 to specific revision (inclusive). */
  def replayStreamTo[R](stream: ID, toRevision: Int)(callback: StreamConsumer[Transaction, R]): Unit =
    replayStreamRange(stream, 0 to toRevision)(callback)

  /** Query across streams, optionally providing a selector to filter results. */
  def query[U](selector: Selector = Everything)(callback: StreamConsumer[Transaction, U]): Unit
  /** Query across streams, optionally providing a selector to filter results. */
  def querySince[U](sinceTick: Long, selector: Selector = Everything)(callback: StreamConsumer[Transaction, U]): Unit

  /** Subscribe to all transactions, if publishing. */
  final def subscribe[U]()(callback: Transaction => U): Subscription = subscribe(Everything)(callback)
  /** Subscribe to selected transactions, if publishing. */
  def subscribe[U](
      selector: StreamsSelector)(
      callback: Transaction => U): Subscription =
    sys.error(s"Subscribe not enabled! Consider applying trait ${classOf[MessageTransportPublishing[_, _]].getName}")

  type CEVT = Class[_ <: EVT]

  /**
   * General selector.
   * Can be used to either select streams,
   * or cherry-pick transactions within
   * streams. The latter will not provide
   * current tick and revision, and should
   * only be used to limit the amount of
   * data on replay.
   */
  sealed abstract class Selector {
    def include(tx: Transaction): Boolean
    def toStreamsSelector: StreamsSelector
  }
  /**
   * Selector for unbroken streams.
   * A selector
   */
  sealed abstract class StreamsSelector extends Selector {
    /** Subset of channels, if any. */
    def channelSubset: Set[Channel]
    def toStreamsSelector = this
  }

  /** All streams, all channels, unbroken. */
  case object Everything extends StreamsSelector {
    def channelSubset: Set[Channel] = Set.empty
    def include(tx: Transaction) = true
  }
  @varargs
  def ChannelSelector(one: Channel, others: Channel*): StreamsSelector =
    new ChannelSelector((one +: others).toSet)
  def ChannelSelector(channels: Seq[Channel]): StreamsSelector =
    new ChannelSelector(channels.toSet)
  /** All streams in the provided channels, unbroken. */
  case class ChannelSelector private[EventSource] (channels: Set[Channel]) extends StreamsSelector {
    //    def this(one: String, others: String*) = this((one +: others).toSet)
    require(channels.nonEmpty)
    def channelSubset: Set[Channel] = channels
    def include(tx: Transaction) = channels.contains(tx.channel)
  }
  def EventSelector(chEvt: (Channel, Set[CEVT]), more: (Channel, Set[CEVT])*): Selector =
    new EventSelector(Map((chEvt :: more.toList): _*))
  def EventSelector(byChannel: java.util.Map[Channel, java.util.Set[CEVT]]): Selector = {
    import collection.JavaConverters._
    new EventSelector(byChannel.asScala.map(e => e._1 -> e._2.asScala.toSet).toMap)
  }
  /**
    * Only transactions containing the provided events.
    * This means broken, incomplete, streams. Should only
    * be used if necessary, i.e. on data sets so large that
    * it would otherwise take too long to process.
    *
    * NOTE:
    *
    *   * Steps should be taken to ensure that the stream
    *     revision number is still current, since this cannot
    *     be guaranteed when consuming incomplete streams.
    *   * Transactions will still contain all events
    *     for that transaction, not just those selected. Only
    *     transactions not including the selected events at all
    *     will be excluded.
    *
    */
  case class EventSelector private[EventSource] (byChannel: Map[Channel, Set[CEVT]])
    extends Selector {
    require(byChannel.nonEmpty)
    require(byChannel.forall(_._2.nonEmpty), s"No events: ${byChannel.filter(_._2.isEmpty).map(_._1).mkString(", ")}")
    def channelSubset = byChannel.keySet
    def include(tx: Transaction) = byChannel.get(tx.channel).exists { set =>
      tx.events.exists { evt =>
        set.exists { cls =>
          cls isAssignableFrom evt.getClass
        }
      }
    }
    def toStreamsSelector: StreamsSelector = ChannelSelector(byChannel.keySet)
  }

  def SingleStreamSelector(stream: ID, channel: Channel): StreamsSelector = new SingleStreamSelector(stream, channel)
  case class SingleStreamSelector private[EventSource] (stream: ID, channel: Channel) extends StreamsSelector {
    def channelSubset: Set[Channel] = Set(channel)
    def include(tx: Transaction) = {
      assert(tx.channel == channel) // Stream ids are unique across channels, thus merely assert
      tx.stream == stream
    }
  }

  object Selector {
    def Everything: StreamsSelector = EventSource.this.Everything
    @varargs
    def apply(ch: Channel, one: CEVT, others: CEVT*): Selector =
      new EventSelector(Map(ch -> (one +: others).toSet))
    @varargs
    def apply(one: Channel, more: Channel*): StreamsSelector =
      ChannelSelector(one, more: _*)
  }

}

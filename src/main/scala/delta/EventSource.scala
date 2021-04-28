package delta

import scala.concurrent.Future

import scuff.Subscription
import scuff.Reduction

import scala.annotation.varargs
import scala.reflect.ClassTag
import scala.annotation.nowarn

/**
  * Event source, a read-only interface.
  * @note For replays/queries, if transaction consumption
  * is non-sequential (concurrent), consider using
  * [[scuff.concurrent.AsyncReduction]] or similar,
  * to ensure proper concurrency behavior.
  */
trait EventSource[ID, EVT] {

  protected implicit val channelTag: ClassTag[Channel] = ClassTag(classOf[String])
  type Channel = delta.Channel
  type Transaction = delta.Transaction[ID, EVT]

  def currRevision(stream: ID): Future[Option[Revision]]
  def maxTick: Future[Option[Tick]]

  /**
    * Replay complete stream.
    * @param stream Stream identifier
    * @param reduction Transaction consumer
    * @return Future result from consumer
    */
  def replayStream[R](stream: ID)(
      reduction: Reduction[Transaction, Future[R]]): Future[R]

  /**
    * Replay stream from provided revision range.
    * @param stream Stream identifier
    * @param revisionRange stream revision range to replay
    * @param reduction Transaction consumer
    * @return Future result from consumer
    */
  def replayStreamRange[R](stream: ID, revisionRange: Range)(
      reduction: Reduction[Transaction, Future[R]]): Future[R]

  /**
    * Replay stream from provided revision to current.
    * @param stream Stream identifier
    * @param fromRevision Stream revision from which to replay
    * @param reduction Transaction consumer
    * @return Future result from consumer
    */
  def replayStreamFrom[R](stream: ID, fromRevision: Revision)(
      reduction: Reduction[Transaction, Future[R]]): Future[R]

  /**
    * Replay stream from revision 0 to specific revision (inclusive).
    * @param stream Stream identifier
    * @param toRevision Stream revision to stop replay
    * @param reduction Transaction consumer
    * @return Future result from consumer
    */
  def replayStreamTo[R](stream: ID, toRevision: Revision)(
      reduction: Reduction[Transaction, Future[R]]): Future[R] =
    replayStreamRange(stream, 0 to toRevision)(reduction)

  /**
    * Query across streams, optionally providing a selector to filter results.
    * @param selector Stream selector, defaults to all streams
    * @param reduction Transaction consumer
    * @return Future result from consumer
    */
  def query[R](
      selector: Selector = Everything)(
      reduction: Reduction[Transaction, Future[R]]): Future[R]

  /**
    * Query across streams, optionally providing a selector to filter results.
    * @param sinceTick Ignore transactions before this tick
    * @param selector Stream selector, defaults to all streams
    * @param reduction Transaction consumer
    * @return Future result from consumer
    */
  def querySince[R](
      sinceTick: Tick, selector: Selector = Everything)(
      reduction: Reduction[Transaction, Future[R]]): Future[R]

  /**
    * Subscribe to transactions for this local instance.
    * @param selector Optional stream selector
    * @param callback Callback function
    */
  def subscribeLocal[U](
      selector: StreamsSelector = Everything)(
      callback: Transaction => U): Subscription

  /**
    * Subscribe to transactions for all instances.
    * @param selector Stream selector, defaults to all
    * @param callback Callback function
    */
  def subscribeGlobal[U](
      selector: StreamsSelector = Everything)(
      callback: Transaction => U): Subscription

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
  final case object Everything extends StreamsSelector {
    def channelSubset: Set[Channel] = Set.empty
    def include(tx: Transaction) = true
  }
  @varargs
  def ChannelSelector(one: Channel, others: Channel*): StreamsSelector =
    new ChannelSelector((one +: others).toSet)
  def ChannelSelector(channels: Iterable[Channel]): StreamsSelector =
    new ChannelSelector(channels.toSet)
  /** All streams in the provided channels, unbroken. */
  @nowarn
  final case class ChannelSelector private[EventSource] (channels: Set[Channel]) extends StreamsSelector {
    require(channels.nonEmpty)
    def channelSubset: Set[Channel] = channels
    def include(tx: Transaction) = channels.contains(tx.channel)
  }
  def EventSelector(chEvt: (Channel, Set[CEVT]), more: (Channel, Set[CEVT])*): Selector =
    new EventSelector(Map((chEvt :: more.toList): _*))
  def EventSelector(byChannel: java.util.Map[Channel, java.util.Set[CEVT]]): Selector = {
    import scala.jdk.CollectionConverters._
    new EventSelector(byChannel.asScala.map(e => e._1 -> e._2.asScala.toSet).toMap)
  }
  /**
    * Only transactions containing the provided events.
    * This means broken, incomplete, streams. Should only
    * be used if necessary, i.e. on data sets so large that
    * it would otherwise take too long to process.
    *
    * @note
    *   - Steps should be taken to ensure that the stream
    *     revision number is still current, since this cannot
    *     be guaranteed when consuming incomplete streams.
    *   - Transactions will still contain all events
    *     for that transaction, not just those selected. Only
    *     transactions not including the selected events at all
    *     will be excluded.
    *
    */
  @nowarn
  final case class EventSelector private[EventSource] (byChannel: Map[Channel, Set[CEVT]])
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

  def SingleStreamSelector(channel: Channel, stream: ID): StreamsSelector = new SingleStreamSelector(stream, channel)
  @nowarn
  final case class SingleStreamSelector(stream: ID, channel: Channel)
  extends StreamsSelector {
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

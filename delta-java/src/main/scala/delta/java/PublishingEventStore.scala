package delta.java

import delta.{
  Tick, Revision, Channel,
  Transaction, EventStore,
  MessageTransport, MessageTransportPublishing
}

import scuff.StreamConsumer

import scala.concurrent.Future

trait PublishingEventStore[ID, EVT]
extends EventStore[ID, EVT]
with MessageTransportPublishing[ID, EVT]

object PublishingEventStore {

  def withPublishing[ID, EVT, M](
      eventStore: EventStore[ID, EVT],
      txTransport: MessageTransport { type TransportType = M },
      txChannels: Set[String],
      txCodec: scuff.Codec[Transaction[ID, EVT], M],
      channelToTopic: java.util.function.Function[String, String]): PublishingEventStore[ID, EVT] = {
    val typedChannels = txChannels.map(Channel(_))
    new EventStoreProxy(eventStore, txTransport, typedChannels, txCodec, channelToTopic) with PublishingEventStore[ID, EVT]
  }

}

private abstract class EventStoreProxy[ID, EVT, M](
  evtStore: EventStore[ID, EVT],
  protected val txTransport: MessageTransport { type TransportType = M },
  protected val txChannels: Set[Channel],
  protected val txCodec: scuff.Codec[delta.Transaction[ID, EVT], M],
  ch2tp: java.util.function.Function[String, String])
extends EventStore[ID, EVT] {
  publishing: MessageTransportPublishing[ID, EVT] =>

  protected def toTopic(ch: Channel) = Topic(ch2tp(ch.toString))

  implicit private def adapt(selector: this.Selector): evtStore.Selector = selector match {
    case Everything => evtStore.Everything
    case ChannelSelector(channels) => evtStore.ChannelSelector(channels)
    case EventSelector(events) => evtStore.EventSelector(events)
    case SingleStreamSelector(stream, channel) => evtStore.SingleStreamSelector.apply(stream, channel)
  }

  def ticker = evtStore.ticker
  def currRevision(stream: ID) = evtStore.currRevision(stream)
  def maxTick = evtStore.maxTick
  def query[U](selector: Selector)(callback: StreamConsumer[Transaction, U]): Unit =
    evtStore.query(selector)(callback)
  def querySince[U](sinceTick: Tick, selector: Selector)(callback: StreamConsumer[Transaction, U]): Unit =
    evtStore.querySince(sinceTick, selector)(callback)
  def replayStream[U](stream: ID)(callback: StreamConsumer[Transaction, U]): Unit =
    evtStore.replayStream(stream)(callback)
  def replayStreamFrom[U](stream: ID, fromRevision: Revision)(callback: StreamConsumer[Transaction, U]): Unit =
    evtStore.replayStreamFrom(stream, fromRevision)(callback)
  def replayStreamRange[U](stream: ID, revisionRange: Range)(callback: StreamConsumer[Transaction, U]): Unit =
    evtStore.replayStreamRange(stream, revisionRange)(callback)

  def commit(channel: Channel, stream: ID, revision: Revision, tick: Tick,
      events: List[EVT], metadata: Map[String, String] = Map.empty): Future[Transaction] =
    evtStore.commit(channel, stream, revision, tick, events, metadata)

}

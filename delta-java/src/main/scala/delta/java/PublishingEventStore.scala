// package delta.java

// import delta.{
//   Tick, Revision, Channel,
//   Transaction, EventStore,
//   MessageTransport, MessageTransportPublishing
// }

// import scuff.Reduction

// import scala.concurrent.Future

// trait PublishingEventStore[ID, EVT]
// extends EventStore[ID, EVT]
// with MessageTransportPublishing[ID, EVT]

// object PublishingEventStore {

//   def withPublishing[ID, EVT, M](
//       eventStore: EventStore[ID, EVT],
//       txTransport: MessageTransport { type InMotion = M },
//       txChannels: Set[String],
//       txTransportCodec: scuff.Codec[Transaction[ID, EVT], M],
//       channelToTopic: java.util.function.Function[String, String]): PublishingEventStore[ID, EVT] = {
//     val typedChannels = txChannels.map(Channel(_))
//     new EventStoreProxy(eventStore, txTransport, typedChannels, txTransportCodec, channelToTopic)
//     with PublishingEventStore[ID, EVT]
//   }

// }

// private abstract class EventStoreProxy[ID, EVT, M](
//   evtStore: EventStore[ID, EVT],
//   protected val txTransport: MessageTransport { type InMotion = M },
//   protected val txChannels: Set[Channel],
//   protected val txTransportCodec: scuff.Codec[delta.Transaction[ID, EVT], M],
//   ch2tp: java.util.function.Function[String, String])
// extends EventStore[ID, EVT] {
//   publishing: MessageTransportPublishing[ID, EVT] =>

//   protected def toTopic(ch: Channel) = Topic(ch2tp(ch.toString))

//   implicit private def adapt(selector: this.Selector): evtStore.Selector = selector match {
//     case Everything => evtStore.Everything
//     case ChannelSelector(channels) => evtStore.ChannelSelector(channels)
//     case EventSelector(events) => evtStore.EventSelector(events)
//     case SingleStreamSelector(stream, channel) => evtStore.SingleStreamSelector.apply(stream, channel)
//   }

//   def ticker = evtStore.ticker
//   def currRevision(stream: ID) = evtStore.currRevision(stream)
//   def maxTick = evtStore.maxTick
//   def query[U](selector: Selector)(reduction: Reduction[Transaction, U]): Future[U] =
//     evtStore.query(selector)(reduction)
//   def querySince[U](sinceTick: Tick, selector: Selector)(reduction: Reduction[Transaction, U]): Future[U] =
//     evtStore.querySince(sinceTick, selector)(reduction)
//   def replayStream[U](stream: ID)(reduction: Reduction[Transaction, U]): Future[U] =
//     evtStore.replayStream(stream)(reduction)
//   def replayStreamFrom[U](stream: ID, fromRevision: Revision)(reduction: Reduction[Transaction, U]): Future[U] =
//     evtStore.replayStreamFrom(stream, fromRevision)(reduction)
//   def replayStreamRange[U](stream: ID, revisionRange: Range)(reduction: Reduction[Transaction, U]): Future[U] =
//     evtStore.replayStreamRange(stream, revisionRange)(reduction)

//   protected def commit(
//       tick: Tick,
//       channel: Channel,
//       stream: ID,
//       revision: Revision,
//       metadata: Map[String, String],
//       events: List[EVT])
//       : Future[Transaction] =
//     evtStore.commit(channel, stream, revision, tick, events, metadata)

// }

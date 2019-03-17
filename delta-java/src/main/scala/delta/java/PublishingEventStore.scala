package delta.java

import delta.EventStore
import delta.MessageHub
import delta.Publishing
import scuff.StreamConsumer
import delta.Transaction, Transaction.Channel
import scala.concurrent.Future

trait PublishingEventStore[ID, EVT] extends EventStore[ID, EVT] with Publishing[ID, EVT]

object PublishingEventStore {

  def withPublishing[ID, EVT](
      eventStore: EventStore[ID, EVT],
      txnHub: MessageHub[Transaction[ID, EVT]],
      txnChannels: Set[String],
      channelToNamespace: java.util.function.Function[String, String]): PublishingEventStore[ID, EVT] = {
    val typedChannels = txnChannels.map(Channel(_))
    new EventStoreProxy(eventStore, txnHub, typedChannels, channelToNamespace) with PublishingEventStore[ID, EVT]
  }
}

private abstract class EventStoreProxy[ID, EVT](
    evtStore: EventStore[ID, EVT],
    protected val txnHub: MessageHub[Transaction[ID, EVT]],
    protected val txnChannels: Set[Channel],
    ch2ns: java.util.function.Function[String, String])
  extends EventStore[ID, EVT] {
  publishing: Publishing[ID, EVT] =>

  protected def toNamespace(ch: Channel) = Namespace(ch2ns(ch.toString))

  import language.implicitConversions
  implicit private def adapt(selector: this.Selector): evtStore.Selector = selector match {
    case Everything => evtStore.Everything
    case ChannelSelector(channels) => evtStore.ChannelSelector(channels)
    case EventSelector(events) => evtStore.EventSelector(events)
    case SingleStreamSelector(stream, channel) => evtStore.SingleStreamSelector.apply(stream, channel)
  }

  def currRevision(stream: ID) = evtStore.currRevision(stream)
  def maxTick() = evtStore.maxTick()
  def query[U](selector: Selector)(callback: StreamConsumer[TXN, U]): Unit =
    evtStore.query(selector)(callback)
  def querySince[U](sinceTick: Long, selector: Selector)(callback: StreamConsumer[TXN, U]): Unit =
    evtStore.querySince(sinceTick, selector)(callback)
  def replayStream[U](stream: ID)(callback: StreamConsumer[TXN, U]): Unit =
    evtStore.replayStream(stream)(callback)
  def replayStreamFrom[U](stream: ID, fromRevision: Int)(callback: StreamConsumer[TXN, U]): Unit =
    evtStore.replayStreamFrom(stream, fromRevision)(callback)
  def replayStreamRange[U](stream: ID, revisionRange: Range)(callback: StreamConsumer[TXN, U]): Unit =
    evtStore.replayStreamRange(stream, revisionRange)(callback)

  def commit(channel: Channel, stream: ID, revision: Int, tick: Long,
      events: List[EVT], metadata: Map[String, String] = Map.empty): Future[TXN] =
    evtStore.commit(channel, stream, revision, tick, events, metadata)

}

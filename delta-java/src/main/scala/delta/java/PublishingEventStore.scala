package delta.java

import delta.EventStore
import delta.Publisher
import delta.Publishing
import scuff.StreamConsumer
import delta.Transaction
import scala.concurrent.Future

trait PublishingEventStore[ID, EVT] extends EventStore[ID, EVT] with Publishing[ID, EVT]

object PublishingEventStore {
  def create[ID, EVT](eventStore: EventStore[ID, EVT], publisher: Publisher[ID, EVT]): PublishingEventStore[ID, EVT] =
    new EventStoreProxy(eventStore, publisher) with PublishingEventStore[ID, EVT]
}

private abstract class EventStoreProxy[ID, EVT](
    evtStore: EventStore[ID, EVT],
    protected val publisher: Publisher[ID, EVT])
  extends EventStore[ID, EVT] {

  import language.implicitConversions
  implicit private def adapt(selector: this.Selector): evtStore.Selector = selector match {
    case Everything => evtStore.Everything
    case ChannelSelector(channels) => evtStore.ChannelSelector(channels)
    case EventSelector(events) => evtStore.EventSelector(events)
    case StreamSelector(stream, channel) => evtStore.StreamSelector.apply(stream, channel)
  }

  def currRevision(stream: ID) = evtStore.currRevision(stream)
  def maxTick() = evtStore.maxTick()
  def query[U](selector: Selector)(callback: StreamConsumer[TXN, U]): Unit =
    evtStore.query(selector)(callback)
  def querySince[U](sinceTick: Long, selector: Selector)(callback: StreamConsumer[TXN, U]): Unit =
    evtStore.querySince(sinceTick, selector)(callback)
  def replayStream[E >: EVT, U](stream: ID)(callback: StreamConsumer[Transaction[ID, E], U]): Unit =
    evtStore.replayStream(stream)(callback)
  def replayStreamFrom[E >: EVT, U](stream: ID, fromRevision: Int)(callback: StreamConsumer[Transaction[ID, E], U]): Unit =
    evtStore.replayStreamFrom(stream, fromRevision)(callback)
  def replayStreamRange[E >: EVT, U](stream: ID, revisionRange: Range)(callback: StreamConsumer[Transaction[ID, E], U]): Unit =
    evtStore.replayStreamRange(stream, revisionRange)(callback)

  def commit(channel: String, stream: ID, revision: Int, tick: Long,
      events: List[EVT], metadata: Map[String, String] = Map.empty): Future[TXN] =
    evtStore.commit(channel, stream, revision, tick, events, metadata)

}

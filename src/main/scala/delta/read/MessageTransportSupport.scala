package delta.read

import scuff.Subscription
import scuff.Codec
import delta.MessageTransport

trait MessageTransportSupport[ID, S, U]
extends SubscriptionSupport[ID, S, U] {
  rm: ReadModel[ID, S] =>

  protected type Topic = MessageTransport.Topic
  protected def Topic(name: String) = MessageTransport.Topic(name)

  protected type TransportType
  protected def transportCodec: Codec[(StreamId, Update), TransportType]
  protected val transport: MessageTransport[TransportType]
  protected def transportTopic: Topic

  protected implicit lazy val encoder = transportCodec.encode _
  private[this] implicit lazy val decoder = transportCodec.decode _

  protected def subscribe(id: ID)(callback: Update => Unit): Subscription = {
    val MatchId = StreamId(id)
    transport.subscribe[(StreamId, Update)](transportTopic) {
      case (MatchId, update) => callback(update)
    }
  }

}

package delta

import scuff._
import scala.reflect.ClassTag
import delta.process.Update

trait MessageHub[ID, M] {
  def publish(id: ID, msg: M): Unit
  def subscribe(id: ID)(callback: M => Unit): Subscription
}

object MessageHub {

  def apply[ID, M](
      transport: MessageTransport { type TransportType = (ID, Update[M]) },
      topic: MessageTransport.Topic): MessageHub[ID, Update[M]] =
    new MessageTransportHub[ID, Update[M], (ID, Update[M])](transport, topic, Codec.noop)

  def apply[ID, M, T: ClassTag](
      transport: MessageTransport { type TransportType = T },
      topic: MessageTransport.Topic,
      codec: Codec[(ID, M), T]): MessageHub[ID, M] =
    new MessageTransportHub[ID, M, T](transport, topic, codec)

  private final class MessageTransportHub[ID, M, T](
    transport: MessageTransport { type TransportType = T },
    topic: MessageTransport.Topic,
    codec: Codec[(ID, M), T])
  extends MessageHub[ID, M] {

    implicit private[this] val encoder = codec.encode _
    implicit private[this] val decoder = codec.decode _

    def publish(id: ID, msg: M): Unit = transport.publish(topic, id -> msg)
    def subscribe(MatchId: ID)(callback: M => Unit): Subscription = {
      transport.subscribe[(ID, M)](topic) {
        case (MatchId, msg) => callback(msg)
      }
    }

  }

}

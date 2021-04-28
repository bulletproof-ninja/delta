package delta

import scuff._
import delta.process.Update

trait MessageSink[ID, -M] {
  def publish(id: ID, msg: M): Unit
}

trait MessageSource[ID, +M] {
  def subscribe(id: ID)(callback: M => Unit): Subscription

  def withUpdate[U](implicit ev: M <:< Update[_]): MessageSource[ID, Update[U]] = {
    val source = MessageSource.this
    new MessageSource[ID, Update[U]] {
      def subscribe(id: ID)(callback: Update[U] => Unit): Subscription =
        source.subscribe(id)(callback.asInstanceOf[M => Unit])
    }
  }

}

trait MessageHub[ID, M]
extends MessageSink[ID, M]
with MessageSource[ID, M]

object MessageHub {

  def forUpdates[ID, M](
      transport: MessageTransport[(ID, Update[M])],
      topic: MessageTransport.Topic)
      : MessageHub[ID, Update[M]] =
    new MessageTransportHub[ID, Update[M], (ID, Update[M])](transport, topic)

  def apply[ID, M, T](
      transport: MessageTransport[T],
      topic: MessageTransport.Topic,
      codec: Codec[(ID, M), T])
      : MessageHub[ID, M] =
    new MessageTransportHub[ID, M, T](transport, topic)(codec.encode, codec.decode)

  def apply[ID, M, T](
      transport: MessageTransport[T],
      topic: MessageTransport.Topic)(
      implicit
      encode: ((ID, M)) => T,
      decode: T => (ID, M))
      : MessageHub[ID, M] =
    new MessageTransportHub[ID, M, T](transport, topic)

  private final class MessageTransportHub[ID, M, T](
    transport: MessageTransport[T],
    topic: MessageTransport.Topic)(
    implicit
    encoder: ((ID, M)) => T,
    decoder: T => (ID, M))
  extends MessageHub[ID, M] {

    def publish(id: ID, msg: M): Unit = transport.publish(topic, id -> msg)
    def subscribe(MatchId: ID)(callback: M => Unit): Subscription = {
      transport.subscribe[(ID, M)](topic) {
        case (MatchId, msg) => callback(msg)
      }
    }

  }

}

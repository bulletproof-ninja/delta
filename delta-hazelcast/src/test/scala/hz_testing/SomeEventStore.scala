package hz_testing

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
import com.hazelcast.core.HazelcastInstance
import delta.MessageTransportPublishing
import delta.hazelcast.TopicMessageTransport
import delta.Channel
import scuff.Codec
import delta.Ticker
import delta.EventSource

class SomeEventStore(ec: ExecutionContext, hz: HazelcastInstance, initTicker: EventSource[Int, MyEvent] => Ticker)
  extends TransientEventStore[Int, MyEvent, Array[Byte]](ec, BinaryEventFormat)(initTicker)
  with MessageTransportPublishing[Int, MyEvent] {

  protected def toTopic(ch: Channel) = Topic(s"tx:$ch")
  protected val txTransport = new TopicMessageTransport[Transaction](hz, ec)
  protected val txChannels = Set(Channel("one"))
  protected val txCodec = Codec.noop[Transaction]

}

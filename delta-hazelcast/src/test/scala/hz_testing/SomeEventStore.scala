package hz_testing

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
// import com.hazelcast.core.HazelcastInstance
// import delta.hazelcast.TopicMessageTransport
import delta._

class SomeEventStore(ec: ExecutionContext, initTicker: EventSource[Int, MyEvent] => Ticker)
extends TransientEventStore[Int, MyEvent, Array[Byte]](ec, BinaryEventFormat) {
  lazy val ticker = initTicker(this)
  // lazy val foo = new TopicMessageTransport[Transaction](hz, ec)
  // protected def toTopic(ch: Channel) = Topic(s"tx:$ch")
  // protected val txTransport = new TopicMessageTransport[Transaction](hz, ec)
  protected val txChannels = Set(Channel("one"))
  // protected val txTransportCodec = Codec.noop[Transaction]

}

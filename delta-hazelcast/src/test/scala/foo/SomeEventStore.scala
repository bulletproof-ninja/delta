package foo

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
import com.hazelcast.core.HazelcastInstance
import delta.MessageHubPublishing
import delta.hazelcast.TopicMessageHub
import delta.Transaction.Channel
import scuff.Codec
import delta.Ticker
import delta.EventSource

class SomeEventStore(ec: ExecutionContext, hz: HazelcastInstance, initTicker: EventSource[Int, MyEvent] => Ticker)
  extends TransientEventStore[Int, MyEvent, Array[Byte]](ec, BinaryEventFormat)(initTicker)
  with MessageHubPublishing[Int, MyEvent] {

  protected def toTopic(ch: Channel) = Topic(s"txn:$ch")
  protected val txnHub = new TopicMessageHub[TXN](hz, ec)
  protected val txnChannels = Set(Channel("one"))
  protected val txnCodec = Codec.noop[TXN]

}

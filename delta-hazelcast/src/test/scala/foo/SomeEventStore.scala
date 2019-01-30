package foo

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
import com.hazelcast.core.HazelcastInstance
import delta.Publishing
import delta.hazelcast.TopicMessageHub
import delta.Transaction.Channel

class SomeEventStore(ec: ExecutionContext, hz: HazelcastInstance)
  extends TransientEventStore[Int, MyEvent, Array[Byte]](ec)
  with Publishing[Int, MyEvent] {

  protected def toNamespace(ch: Channel) = Namespace(s"txn:$ch")
  protected val txnHub = TopicMessageHub[TXN](hz, ec)
  protected val txnChannels = Set(Channel("one"))
}

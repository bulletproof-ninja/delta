package foo

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
import delta.hazelcast.TopicPublisher
import com.hazelcast.core.HazelcastInstance
import delta.Publishing
import delta.Publisher

class SomeEventStore(ec: ExecutionContext, hz: HazelcastInstance)
  extends TransientEventStore[Int, MyEvent, Unit, Array[Byte]](ec)
  with Publishing[Int, MyEvent, Unit] {

  protected val publisher: Publisher[Int, MyEvent, Unit] =
    TopicPublisher(hz, Set(()), ec, ch => s"SomeEventStore:topic($ch)")

}

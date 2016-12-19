
import sampler.aggr.DomainEvent
import scuff.serialVersionUID
import ulysses.EventCodec
import ulysses.util.ReflectiveDecoder
import language.implicitConversions
import sampler.aggr.dept.DeptEvent
import sampler.aggr.emp.EmpEvent
import sampler.aggr.Employee
import sampler.aggr.Department
import scala.util.Random
import scala.concurrent._, duration._

package object sampler {

  val isDebug = java.lang.management.ManagementFactory
    .getRuntimeMXBean
    .getInputArguments
    .toString.contains("jdwp")

  val AwaitDuration = if (isDebug) 10.hours else 10.seconds

  implicit class F[T](f: Future[T]) {
    def await = Await.result(f, AwaitDuration)
  }

  type JSON = String

  implicit def toFuture[T](t: T): Future[T] = Future successful t
  
  case class Id[T](int: Int = Random.nextInt)
  type DeptId = Id[Department]
  type EmpId = Id[Employee]

  implicit def id2int(id: Id[_]) = id.int

  trait AbstractEventCodec[SF]
      extends EventCodec[DomainEvent, SF] {

    private[this] val evtName = new ClassValue[String] {
      def computeValue(cls: Class[_]) = {
        val fullName = cls.getName
        val sepIdx = fullName.lastIndexOf('.', fullName.lastIndexOf('.') - 1)
        fullName.substring(sepIdx + 1)
      }
    }

    def name(cls: EventClass): String = evtName.get(cls)
    def version(cls: EventClass): Byte = serialVersionUID(cls).toByte

  }

  implicit object JsonDomainEventCodec
      extends ReflectiveDecoder[DomainEvent, JSON]
      with AbstractEventCodec[JSON]
      with aggr.emp.JsonCodec
      with aggr.dept.JsonCodec {

    override type RT = JSON

    def encode(evt: DomainEvent) = evt match {
      case evt: aggr.dept.DeptEvent => evt.dispatch(this)
      case evt: aggr.emp.EmpEvent => evt.dispatch(this)
    }
  }

}

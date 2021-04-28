package delta.testing

import delta._
import delta.process.StreamProcessStore
import delta.read.impl.IncrementalReadModel
import delta.read.UnknownIdRequested

import java.util.UUID
import java.util.concurrent.Executors

import scuff.concurrent.PartitionedExecutionContext

import scala.concurrent._, duration._
import scala.reflect.{ ClassTag, classTag }
import delta.process.LiveProcessConfig

object TestReadModels {
  case class Foo(name: String, age: Int)
  object FooProjector
  extends Projector[Foo, Event] {
    import Event._

    def init(evt: Event): Foo = evt match {
      case evt: Genesis => new Foo(evt.name, evt.age)
      case _ => ???
    }

    def next(state: Foo, evt: Event): Foo = evt match {
      case evt: NameChanged => state.copy(name = evt.newName)
      case evt: AgeChanged => state.copy(age = evt.newAge)
      case _ => ???
    }
  }

  val Scheduler = Executors.newScheduledThreadPool(2)
}

class TestReadModels
extends BaseTest {

  import TestReadModels._

  def newEventStore() =
    new util.TransientEventStore[UUID, Event, Array[Byte]](
      ec, EvtFmt) {
      def ticker: Ticker = SysClockTicker
    }
  def newProcessStore[S <: AnyRef: ClassTag](): StreamProcessStore[UUID, S, S] =
    new InMemoryProcStore[UUID, S, S](classTag[S].runtimeClass.getSimpleName) {

    }

  test("IncrementalReadModel must work with empty store") {
    val es = newEventStore()
    val procStore = newProcessStore[Foo]()
    val rm = new IncrementalReadModel[UUID, UUID, Event, Foo](es) {
      protected def projector(tx: Transaction) = FooProjector
      protected def updateHub = NoopMessageHub[UUID, Update]
      private val pec = PartitionedExecutionContext(1, _.printStackTrace())
      protected def processContext(id: UUID): ExecutionContext = pec.singleThread(id.##)
      protected def scheduler = Scheduler
      protected def processStore = procStore
      protected def onMissingRevision = LiveProcessConfig.ImmediateReplay
    }
    val id = UUID.randomUUID
    try rm.read(id).await catch {
      case e: UnknownIdRequested =>
        assert(id === e.id)
        assert(e.isInstanceOf[TimeoutException])
    }
    try rm.read(id, 100.millis).await catch {
      case e: UnknownIdRequested =>
        assert(id === e.id)
        assert(e.isInstanceOf[TimeoutException])
    }
    try rm.read(id, minRevision = 5, timeout = 100.millis).await catch {
      case e: UnknownIdRequested =>
        assert(id === e.id)
        assert(e.isInstanceOf[TimeoutException])
    }
    try rm.read(id, minRevision = 5).await catch {
      case e: UnknownIdRequested =>
        assert(id === e.id)
        assert(e.isInstanceOf[TimeoutException])
    }
  }

}

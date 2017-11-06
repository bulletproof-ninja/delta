package delta.testing

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.{ MILLISECONDS, SECONDS }
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Seq
import scala.concurrent.{ Future, Promise }
import scala.concurrent.Await
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.util.{ Failure, Success, Try }
import org.junit._
import org.junit.Assert._
import rapture.json.jsonStringContext
import scuff._
import delta.ddd.{ Repository, UnknownIdException }
import scuff.reflect.Surgeon
import delta.ddd._
import delta.util._
import delta.util.LocalPublishing
import delta.EventCodec
import delta.EventStore
import delta.NoVersioning
import delta.SysClockTicker
import scala.{ SerialVersionUID => version }
import delta.Fold
import delta.Snapshot

trait AggrEventHandler {
  type Return
  def dispatch(evt: AggrEvent): Return = evt.dispatch(this)

  def on(evt: AggrCreated): Return
  def on(evt: NewNumberWasAdded): Return
  def on(evt: StatusChanged): Return
}

case class AddNewNumber(n: Int)
case class ChangeStatus(newStatus: String)

case class AggrState(status: String, numbers: Set[Int])

final class AggrStateMutator
  extends StateMutator {

  type Event = AggrEvent
  type State = AggrState

  val fold = new Fold[State, Event] {
    def init(evt: Event) = new EvtHandler().dispatch(evt)
    def next(s: State, evt: Event) = new EvtHandler(s).dispatch(evt)
  }

  def theAppliedEvents = {
    val surgeon = new Surgeon(this)
    surgeon.getAll[List[AggrEvent]].head._2.reverse
  }

  //  def process(evt: AggrEvent) = dispatch(evt)

  private class EvtHandler(state: AggrState = null) extends AggrEventHandler {
    type Return = AggrState

    def on(evt: AggrCreated) = {
      require(state == null)
      new AggrState(evt.status, Set.empty)
    }
    def on(evt: NewNumberWasAdded) = {
      val numbers = state.numbers + evt.n
      state.copy(numbers = numbers)
    }
    def on(evt: StatusChanged) = {
      state.copy(status = evt.newStatus)
    }
  }
}

sealed abstract class AggrEvent extends DoubleDispatch {
  type Callback = AggrEventHandler
}

@version(1)
case class NewNumberWasAdded(n: Int)
  extends AggrEvent { def dispatch(cb: AggrEventHandler): cb.Return = cb.on(this) }
@version(1)
case class AggrCreated(status: String)
  extends AggrEvent { def dispatch(cb: AggrEventHandler): cb.Return = cb.on(this) }
@version(1)
case class StatusChanged(newStatus: String)
  extends AggrEvent { def dispatch(cb: AggrEventHandler): cb.Return = cb.on(this) }

abstract class AbstractEventStoreRepositoryTest {

  implicit def ec = RandomDelayExecutionContext
  implicit def ticker = SysClockTicker

  class TimestampCodec(name: String) extends Codec[Timestamp, Map[String, String]] {
    def encode(ts: Timestamp): Map[String, String] = Map(name -> ts.toString)
    def decode(map: Map[String, String]): Timestamp = Timestamp.parseISO(map(name)).get
  }

  @volatile var es: EventStore[String, AggrEvent, Unit] = _
  @volatile var repo: Repository[String, Aggr] with MutableState[String, Aggr] = _

  private def doAsync(f: Promise[Any] => Unit) {
    val something = Promise[Any]
    f(something)
    Await.result(something.future, 50000.seconds) match {
      case th: Throwable => throw th
      case Failure(th) => throw th
      case _ =>
    }
  }

  def metadata: Map[String, String] = Map(
    "timestamp" -> new Timestamp().toString,
    "random" -> math.random.toString)

  @Test
  def loadUnknownId() = doAsync { done =>
    repo.load("Foo").onComplete {
      case Success(_) => done.complete(Try(fail("Should have failed as unknown")))
      case Failure(e: UnknownIdException) =>
        assertEquals("Foo", e.id)
        done.success(Unit)
      case Failure(other) => done.failure(other)
    }
  }

  @Test
  def failedInvariants() = doAsync { done =>
    val id = "Foo"
    val newFoo = TheOneAggr.create()
    newFoo apply AddNewNumber(-1)
    repo.insert(id, newFoo, metadata).onComplete {
      case Failure(_) =>
        repo.exists(id).onSuccess {
          case None => done.success("Fail on negative number")
          case Some(rev) => fail(s"Should not exist: $rev")
        }
      case Success(_) => fail("Should not accept negative numbers")
    }
  }

  @Test
  def saveNewThenUpdate() = doAsync { done =>
    val id = "Foo"
    val newFoo = TheOneAggr.create()
    repo.insert(id, newFoo, metadata).onSuccess {
      case _ =>
        repo.update("Foo", Revision(0), metadata) {
          case (foo, rev) =>
            assertEquals(0, rev)
            assertEquals("New", foo.aggr.status)
        }.onComplete {
          case Failure(t) => done.failure(t)
          case Success((_, rev)) =>
            assertEquals(0, rev)
            repo.update(id, Revision.Exactly(0)) {
              case (foo, rev) =>
                assertEquals(0, rev)
                assertEquals("New", foo.aggr.status)
                foo(AddNewNumber(44))
            }.onComplete {
              case Failure(t) => done.failure(t)
              case Success((_, rev)) =>
                assertEquals(1, rev)
                repo.update("Foo", Revision(0)) {
                  case (foo, rev) =>
                    assertEquals(1, rev)
                    assertTrue(foo.mergeEvents.contains(NewNumberWasAdded(44)))
                    assertEquals("New", foo.aggr.status)
                    foo(AddNewNumber(44))
                }.onComplete {
                  case Failure(t) => done.failure(t)
                  case Success((_, rev)) =>
                    assertEquals(1, rev)
                    repo.update("Foo", Revision(1), metadata) {
                      case (foo, rev) =>
                        assertEquals(1, rev)
                        assertTrue(foo.mergeEvents.isEmpty)
                        assertEquals("New", foo.aggr.status)
                        foo(ChangeStatus("NotNew"))
                    }.onComplete {
                      case Failure(t) => done.failure(t)
                      case Success((_, rev)) =>
                        assertEquals(2, rev)
                        repo.load("Foo").onSuccess {
                          case (foo, rev) =>
                            assertEquals(2, rev)
                            assertEquals("NotNew", foo.aggr.status)
                            done.success(Unit)
                        }
                    }
                }

            }
        }
    }
  }
  @Test
  def update() = doAsync { done =>
    val id = "Foo"
    val newFoo = TheOneAggr.create()
    newFoo(AddNewNumber(42))
    newFoo.appliedEvents match {
      case Seq(AggrCreated(_), NewNumberWasAdded(n)) => assertEquals(42, n)
      case _ => fail("Event sequence incorrect: " + newFoo.appliedEvents)
    }
    val update1 = repo.insert(id, newFoo).flatMap {
      case _ =>
        repo.update(id, Revision(0)) {
          case (foo, rev) =>
            assertEquals(0, rev)
            foo(AddNewNumber(42))
            assertEquals(0, foo.appliedEvents.size)
            foo(AddNewNumber(99))
            assertEquals(1, foo.appliedEvents.size)
        }
    }
    update1.onComplete {
      case Failure(t) => done.failure(t)
      case Success((_, revision)) =>
        assertEquals(1, revision)
        repo.load(id).onComplete {
          case Failure(t) => done.failure(t)
          case Success((foo, rev)) =>
            assertEquals(1, rev)
            assertTrue(foo.numbers.contains(42))
            assertTrue(foo.numbers.contains(99))
            assertEquals(2, foo.numbers.size)
            done.success(Unit)
        }
    }
  }

  @Test
  def `idempotent insert`() = doAsync { done =>
    val id = "Baz"
    val baz = TheOneAggr.create()
    repo.insert(id, baz).onSuccess {
      case rev0 =>
        assertEquals(0, rev0)
        repo.insert(id, baz).onSuccess {
          case rev0 =>
            assertEquals(0, rev0)
            done.success(Unit)
        }
    }
  }

  @Test
  def `concurrent update`() = doAsync { done =>
    val executor = java.util.concurrent.Executors.newScheduledThreadPool(16)
    val id = "Foo"
    val foo = TheOneAggr.create()
    val insFut = repo.insert(id, foo, metadata)
    val updateRevisions = new TrieMap[Int, Future[Int]]
    val range = 0 to 75
    val latch = new CountDownLatch(range.size)
    insFut.onComplete {
      case f: Failure[_] => done.complete(f)
      case Success(_) =>
        for (i ← range) {
          val runThis = new Runnable {
            def run {
              val fut = repo.update(id, Revision(0), metadata) {
                case (foo, _) =>
                  foo(AddNewNumber(i))
                  Future successful foo
              }
              updateRevisions += i -> fut.map(_._2)
              latch.countDown()
            }
          }
          executor.schedule(runThis, 500, MILLISECONDS)
        }
        if (!latch.await(30, SECONDS)) {
          done.complete(Try(fail("Timed out waiting for concurrent updates to finish")))
        } else {
          assertEquals(range.size, updateRevisions.size)
          val revisions = updateRevisions.map {
            case (_, f) => Await.result(f, Duration.Inf)
          }.toSeq.sorted
          done.complete(Try(assertEquals((1 to range.size).toSeq, revisions)))
        }
    }
  }
  @Test
  def `noop update`() = doAsync { done =>
    val id = "Foo"
    val foo = TheOneAggr.create()
    repo.insert(id, foo).onComplete {
      case Failure(t) => done.failure(t)
      case Success(_) =>
        repo.update(id, Revision(0)) {
          case (foo, rev) =>
            assertEquals(0, rev)
            Future successful foo
        }.onComplete {
          case Failure(t) => done.failure(t)
          case Success((_, newRevision)) =>
            assertEquals(0, newRevision)
            done.success(Unit)
        }
    }
  }
}

class Aggr(val stateMutator: AggrStateMutator, val mergeEvents: Seq[AggrEvent]) {
  def appliedEvents = stateMutator.theAppliedEvents
  private[delta] def aggr = stateMutator.state
  def apply(cmd: AddNewNumber) {
    if (!aggr.numbers.contains(cmd.n)) {
      stateMutator(NewNumberWasAdded(cmd.n))
    }
  }
  def apply(cmd: ChangeStatus) {
    if (aggr.status != cmd.newStatus) {
      stateMutator(StatusChanged(cmd.newStatus))
    }
  }
  def numbers = aggr.numbers
}

object TheOneAggr extends Entity {

  type Id = String
  type Type = Aggr
  type Mutator = AggrStateMutator

  def newMutator = new AggrStateMutator
  def init(m: Mutator, mergeEvents: List[Mutator#Event]): Type = new Aggr(m, mergeEvents)
  def done(entity: Type) = entity.stateMutator
  override def checkInvariants(state: Mutator#State): Unit = {
    require(state.numbers.filter(_ < 0).isEmpty, "Cannot contain negative numbers")
  }
  def create(): Aggr = {
    val mutator = new AggrStateMutator
    mutator(new AggrCreated("New"))
    new Aggr(mutator, Nil)
  }
}

class TestEventStoreRepositoryNoSnapshots extends AbstractEventStoreRepositoryTest {

  implicit object Codec
    extends EventCodec[AggrEvent, String]
    with NoVersioning[AggrEvent, String] {

    import rapture.json._, jsonBackends.jackson._

    def name(cls: EventClass): String = cls.getSimpleName

    def encode(evt: AggrEvent): String = evt match {
      case AggrCreated(status) => json""" { "status": $status } """.toBareString
      case NewNumberWasAdded(num) => json""" { "num": $num } """.toBareString
      case StatusChanged(newStatus) => json""" { "status": $newStatus } """.toBareString
    }
    def decode(name: String, json: String): AggrEvent = {
      val ast = Json.parse(json)
      name match {
        case "AggrCreated" => AggrCreated(status = ast.status.as[String])
        case "NewNumberWasAdded" => NewNumberWasAdded(n = ast.num.as[Int])
        case "StatusChanged" => StatusChanged(newStatus = ast.status.as[String])
      }
    }
  }

  @Before
  def setup() {

    es = new TransientEventStore[String, AggrEvent, Unit, String](
      RandomDelayExecutionContext) with LocalPublishing[String, AggrEvent, Unit] {
      def publishCtx = ec
    }
    repo = new EntityRepository((), TheOneAggr)(es)
  }

}

class TestEventStoreRepositoryWithSnapshots extends AbstractEventStoreRepositoryTest {

  implicit object Codec
    extends ReflectiveDecoder[AggrEvent, String]
    with AggrEventHandler
    with EventCodec[AggrEvent, String]
    with NoVersioning[AggrEvent, String] {

    type Return = String

    import rapture.json._, jsonBackends.jackson._

    def name(cls: EventClass): String = cls.getSimpleName

    def encode(evt: AggrEvent): String = evt.dispatch(this)

    def on(evt: AggrCreated): Return = json""" { "status": ${evt.status} } """.toBareString
    def decodeAggrCreated(json: String): AggrCreated = {
      val ast = Json.parse(json)
      AggrCreated(status = ast.status.as[String])
    }

    def on(evt: NewNumberWasAdded): Return = json""" { "num": ${evt.n} } """.toBareString
    def decodeNewNumberWasAdded(json: String): NewNumberWasAdded = {
      val ast = Json.parse(json)
      NewNumberWasAdded(n = ast.num.as[Int])
    }

    def on(evt: StatusChanged): Return = json""" { "status": ${evt.newStatus} } """.toBareString
    def decodeStatusChanged(json: String): StatusChanged = {
      val ast = Json.parse(json)
      StatusChanged(newStatus = ast.status.as[String])
    }
  }

  @Before
  def setup() {
      def ?[T](implicit t: T) = implicitly[T]
    es = new TransientEventStore[String, AggrEvent, Unit, String](
      RandomDelayExecutionContext) with LocalPublishing[String, AggrEvent, Unit] {
      def publishCtx = RandomDelayExecutionContext
    }
    val snapshotMap = new collection.concurrent.TrieMap[String, Option[Snapshot[AggrState]]]
    val snapshotStore = new ConcurrentMapStore[String, AggrState](snapshotMap, _ => Future successful None)
    repo = new EntityRepository((), TheOneAggr)(es, snapshotStore)(?, RandomDelayExecutionContext, SysClockTicker)
  }

}

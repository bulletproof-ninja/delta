package ulysses.testing

import scala.language.implicitConversions
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.{ MILLISECONDS, SECONDS }
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Seq
import scala.concurrent.{ Future, Promise }
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.util.{ Failure, Success, Try }
import org.junit._
import org.junit.Assert._
import rapture.json.jsonBackends.jackson._
import rapture.json.jsonStringContext
import scuff._
import scuff.ddd.{ Repository, UnknownIdException }
import scuff.reflect.Surgeon
import ulysses.ddd._
import ulysses.util._
import ulysses.util.LocalPublishing
import ulysses.EventCodec
import ulysses.EventStore
import ulysses.NoVersioning
import ulysses.SystemClock
import scala.{ SerialVersionUID => version }

trait AggrEventHandler {
  type RT
  def process(evt: AggrEvent): RT = evt.dispatch(this)

  def on(evt: AggrCreated): RT
  def on(evt: NewNumberWasAdded): RT
  def on(evt: StatusChanged): RT
}

case class AddNewNumber(n: Int)
case class ChangeStatus(newStatus: String)

case class AggrState(status: String, numbers: Set[Int])

final class AggrStateMutator(var state: AggrState = null)
    extends AggrEventHandler
    with StateMutator[AggrEvent, AggrState] {
  def theAppliedEvents = {
    val surgeon = new Surgeon(this)
    surgeon.getAll[List[AggrEvent]].head._2.reverse
  }

  //  def process(evt: AggrEvent) = dispatch(evt)

  type RT = Unit

  def on(evt: AggrCreated) {
    require(state == null)
    state = new AggrState(evt.status, Set.empty)
  }
  def on(evt: NewNumberWasAdded) {
    val numbers = state.numbers + evt.n
    state = state.copy(numbers = numbers)
  }
  def on(evt: StatusChanged) {
    state = state.copy(status = evt.newStatus)
  }

}

sealed abstract class AggrEvent extends DoubleDispatch[AggrEventHandler]
@version(1)
case class NewNumberWasAdded(n: Int)
  extends AggrEvent { def dispatch(cb: AggrEventHandler): cb.RT = cb.on(this) }
@version(1)
case class AggrCreated(status: String)
  extends AggrEvent { def dispatch(cb: AggrEventHandler): cb.RT = cb.on(this) }
@version(1)
case class StatusChanged(newStatus: String)
  extends AggrEvent { def dispatch(cb: AggrEventHandler): cb.RT = cb.on(this) }

abstract class AbstractEventStoreRepositoryTest {

  class TimestampCodec(name: String) extends Codec[Timestamp, Map[String, String]] {
    def encode(ts: Timestamp): Map[String, String] = Map(name -> ts.toString)
    def decode(map: Map[String, String]): Timestamp = Timestamp.parseISO(map(name)).get
  }

  @volatile var es: EventStore[String, AggrEvent, Unit] = _
  @volatile var repo: Repository[String, Aggr] = _

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
  def loadUnknownId = doAsync { done =>
    repo.load("Foo").onComplete {
      case Success(_) => done.complete(Try(fail("Should have failed as unknown")))
      case Failure(e: UnknownIdException) =>
        assertEquals("Foo", e.id)
        done.success(Unit)
      case Failure(other) => done.failure(other)
    }
  }

  @Test
  def failedInvariants = doAsync { done =>
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
  def saveNewThenUpdate = doAsync { done =>
    val id = "Foo"
    val newFoo = TheOneAggr.create()
    repo.insert(id, newFoo, metadata).onSuccess {
      case _ =>
        repo.update("Foo", 0, metadata) {
          case (foo, rev) =>
            assertEquals(0, rev)
            assertEquals("New", foo.aggr.status)
            Future successful foo
        }.onComplete {
          case Failure(t) => done.failure(t)
          case Success(rev) =>
            assertEquals(0, rev)
            repo.update("Foo", 0) {
              case (foo, rev) =>
                assertEquals(0, rev)
                assertEquals("New", foo.aggr.status)
                foo(AddNewNumber(44))
                Future successful foo
            }.onComplete {
              case Failure(t) => done.failure(t)
              case Success(rev) =>
                assertEquals(1, rev)
                repo.update("Foo", 0) {
                  case (foo, rev) =>
                    assertEquals(1, rev)
                    assertTrue(foo.mergeEvents.contains(NewNumberWasAdded(44)))
                    assertEquals("New", foo.aggr.status)
                    foo(AddNewNumber(44))
                    Future successful foo
                }.onComplete {
                  case Failure(t) => done.failure(t)
                  case Success(rev) =>
                    assertEquals(1, rev)
                    repo.update("Foo", 1, metadata) {
                      case (foo, rev) =>
                        assertEquals(1, rev)
                        assertTrue(foo.mergeEvents.isEmpty)
                        assertEquals("New", foo.aggr.status)
                        foo(ChangeStatus("NotNew"))
                        Future successful foo
                    }.onComplete {
                      case Failure(t) => done.failure(t)
                      case Success(rev) =>
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
  def update = doAsync { done =>
    val id = "Foo"
    val newFoo = TheOneAggr.create()
    newFoo(AddNewNumber(42))
    newFoo.appliedEvents match {
      case Seq(AggrCreated(_), NewNumberWasAdded(n)) => assertEquals(42, n)
      case _ => fail("Event sequence incorrect: " + newFoo.appliedEvents)
    }
    val update1 = repo.insert(id, newFoo).flatMap {
      case _ =>
        repo.update("Foo", 0) {
          case (foo, rev) =>
            assertEquals(0, rev)
            foo(AddNewNumber(42))
            assertEquals(0, foo.appliedEvents.size)
            foo(AddNewNumber(99))
            assertEquals(1, foo.appliedEvents.size)
            Future successful foo
        }
    }
    update1.onComplete {
      case Failure(t) => done.failure(t)
      case Success(revision) =>
        assertEquals(1, revision)
        repo.load("Foo").onComplete {
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
  def `idempotent insert` = doAsync { done =>
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
  def `concurrent update` = doAsync { done =>
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
              val fut = repo.update("Foo", 0, metadata) {
                case (foo, rev) =>
                  foo(AddNewNumber(i))
                  Future successful foo
              }
              updateRevisions += i -> fut
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
            case (i, f) => Await.result(f, Duration.Inf)
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
        repo.update("Foo", 0) {
          case (foo, rev) => Future successful foo
        }.onComplete {
          case Failure(t) => done.failure(t)
          case Success(newRevision) =>
            assertEquals(0, newRevision)
            done.success(Unit)
        }
    }
  }
}

class Aggr(val stateMutator: AggrStateMutator, val mergeEvents: Seq[AggrEvent]) {
  def appliedEvents = stateMutator.theAppliedEvents
  private[ulysses] def aggr = stateMutator.state
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

object TheOneAggr extends AggregateRoot {

  type Id = String
  type Channel = Unit
  def channel = ()
  type Entity = Aggr
  type Event = AggrEvent
  type State = AggrState

  def newMutator(state: Option[State]): StateMutator[Event, State] = new AggrStateMutator(state.orNull)
  def init(state: State, mergeEvents: List[Event]): Entity = new Aggr(new AggrStateMutator(state), mergeEvents)
  def done(entity: Entity): StateMutator[Event, State] = entity.stateMutator
  def checkInvariants(state: State): Unit = {
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
  def setup {
    es = new TransientEventStore[String, AggrEvent, Unit, String](
      RandomDelayExecutionContext) with LocalPublishing[String, AggrEvent, Unit] {
      def publishCtx = RandomDelayExecutionContext
    }
    repo = new EntityRepository(global, SystemClock, TheOneAggr)(es)
  }

}

class TestEventStoreRepositoryWithSnapshots extends AbstractEventStoreRepositoryTest {

  implicit object Codec
      extends ReflectiveDecoder[AggrEvent, String]
      with AggrEventHandler
      with EventCodec[AggrEvent, String]
      with NoVersioning[AggrEvent, String] {

    type RT = String

    import rapture.json._, jsonBackends.jackson._

    def name(cls: EventClass): String = cls.getSimpleName

    def encode(evt: AggrEvent): String = evt.dispatch(this)

    def on(evt: AggrCreated): RT = json""" { "status": ${evt.status} } """.toBareString
    def decodeAggrCreated(json: String): AggrCreated = {
      val ast = Json.parse(json)
      AggrCreated(status = ast.status.as[String])
    }

    def on(evt: NewNumberWasAdded): RT = json""" { "num": ${evt.n} } """.toBareString
    def decodeNewNumberWasAdded(json: String): NewNumberWasAdded = {
      val ast = Json.parse(json)
      NewNumberWasAdded(n = ast.num.as[Int])
    }

    def on(evt: StatusChanged): RT = json""" { "status": ${evt.newStatus} } """.toBareString
    def decodeStatusChanged(json: String): StatusChanged = {
      val ast = Json.parse(json)
      StatusChanged(newStatus = ast.status.as[String])
    }
  }

  @Before
  def setup {
    es = new TransientEventStore[String, AggrEvent, Unit, String](
      RandomDelayExecutionContext) with LocalPublishing[String, AggrEvent, Unit] {
      def publishCtx = RandomDelayExecutionContext
    }
    val snapshotMap = new collection.concurrent.TrieMap[String, SnapshotStore[String, AggrState]#Snapshot]
    val snapshotStore = new MapSnapshotStore[Aggr, String, AggrEvent, AggrState](snapshotMap)
    repo = new EntityRepository(global, SystemClock, TheOneAggr)(es, snapshotStore)
  }

}

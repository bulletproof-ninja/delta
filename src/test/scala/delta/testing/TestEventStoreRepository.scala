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
import scuff._
import delta.ddd.{ Repository, UnknownIdException }
import scuff.reflect.Surgeon
import delta._
import delta.ddd._
import delta.util._
import scala.{ SerialVersionUID => version }
import delta.process.ConcurrentMapStore

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

object AggrStateProjector extends Projector[AggrState, AggrEvent] {
  def init(evt: AggrEvent) = new EvtHandler().dispatch(evt)
  def next(s: AggrState, evt: AggrEvent) = new EvtHandler(s).dispatch(evt)
}

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
  def ticker = SysClockTicker

  class TimestampCodec(name: String) extends Codec[Timestamp, Map[String, String]] {
    def encode(ts: Timestamp): Map[String, String] = Map(name -> ts.toString)
    def decode(map: Map[String, String]): Timestamp = Timestamp.parseISO(map(name)).get
  }

  @volatile var es: EventStore[String, AggrEvent] = _
  @volatile var repo: Repository[String, Aggr] with MutableEntity = _

  private def doAsync(f: Promise[Any] => Unit): Unit = {
    val something = Promise[Any]
    f(something)
    Await.result(something.future, 222.seconds) match {
      case th: Throwable => throw th
      case Failure(th) => throw th
      case _ =>
    }
  }

  implicit def metadata: Metadata = Metadata(
    "timestamp" -> new Timestamp().toString,
    "random" -> math.random.toString)

  @Test
  def loadUnknownId() = doAsync { done =>
    repo.load("Foo").onComplete {
      case Success(_) => done.complete(Try(fail("Should have failed as unknown")))
      case Failure(e: UnknownIdException) =>
        assertEquals("Foo", e.id)
        done.success(())
      case Failure(other) => done.failure(other)
    }
  }

  @Test
  def failedInvariants() = doAsync { done =>
    val id = "Foo"
    val newFoo = TheOneAggr.create()
    newFoo apply AddNewNumber(-1)
    repo.insert(id, newFoo).onComplete {
      case Failure(_) =>
        repo.exists(id).onComplete {
          case Failure(t) => done.failure(t)
          case Success(None) => done.success("Fail on negative number")
          case Success(Some(rev)) => fail(s"Should not exist: $rev")
        }
      case Success(_) => fail("Should not accept negative numbers")
    }
  }

  @Test
  def saveNewThenUpdate() = doAsync { done =>
    val id = "Foo"
    val newFoo = TheOneAggr.create()
    repo.insert(id, newFoo).onComplete {
      case Failure(t) => done.failure(t)
      case Success(_) =>
        repo.update("Foo", Some(0)) {
          case (foo, rev) =>
            assertEquals(0, rev)
            assertEquals("New", foo.aggr.status)
        }.onComplete {
          case Failure(t) => done.failure(t)
          case Success((_, rev)) =>
            assertEquals(0, rev)
            repo.update(id, Some(0)) {
              case (foo, rev) if rev == 0 =>
                assertEquals(0, rev)
                assertEquals("New", foo.aggr.status)
                foo(AddNewNumber(44))
            }.onComplete {
              case Failure(t) => done.failure(t)
              case Success((_, rev)) =>
                assertEquals(1, rev)
                repo.update("Foo", Some(0)) {
                  case (foo, rev) =>
                    assertEquals(1, rev)
                    assertTrue(foo.mergeEvents.contains(NewNumberWasAdded(44)))
                    assertEquals("New", foo.aggr.status)
                    foo(AddNewNumber(44))
                }.onComplete {
                  case Failure(t) => done.failure(t)
                  case Success((_, rev)) =>
                    assertEquals(1, rev)
                    repo.update("Foo", Some(1)) {
                      case (foo, rev) =>
                        assertEquals(1, rev)
                        assertTrue(foo.mergeEvents.isEmpty)
                        assertEquals("New", foo.aggr.status)
                        foo(ChangeStatus("NotNew"))
                    }.onComplete {
                      case Failure(t) => done.failure(t)
                      case Success((_, rev)) =>
                        assertEquals(2, rev)
                        repo.load("Foo").onComplete {
                          case Failure(t) => done.failure(t)
                          case Success((foo, rev)) =>
                            assertEquals(2, rev)
                            assertEquals("NotNew", foo.aggr.status)
                            done.success(())
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
        repo.update(id, Some(0)) {
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
            done.success(())
        }
    }
  }

  @Test
  def `idempotent insert`() = doAsync { done =>
    val id = "Baz"
    val baz = TheOneAggr.create()
    repo.insert(id, baz).onComplete {
      case Failure(t) => done.failure(t)
      case Success(idAgain) =>
        assertEquals(id, idAgain)
        repo.insert(id, baz).onComplete {
          case Failure(t) => done.failure(t)
          case Success(idAgain) =>
            assertEquals(id, idAgain)
            done.success(())
        }
    }
  }

  @Test
  def `concurrent update`() = doAsync { done =>
    val executor = java.util.concurrent.Executors.newScheduledThreadPool(16)
    val id = "Foo"
    val foo = TheOneAggr.create()
    val insFut = repo.insert(id, foo)
    val updateRevisions = new TrieMap[Int, Future[Int]]
    val range = 0 to 75
    val latch = new CountDownLatch(range.size)
    insFut.onComplete {
      case f: Failure[_] => done.complete(f)
      case Success(_) =>
        for (i <- range) {
          val runThis = new Runnable {
            def run: Unit = {
              val fut = repo.update(id, Some(0)) {
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
          }.toList.sorted
          done.complete(Try(assertEquals((1 to range.size).toList, revisions)))
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
        repo.update(id, Some(0)) {
          case (foo, rev) =>
            assertEquals(0, rev)
            Future successful foo
        }.onComplete {
          case Failure(t) => done.failure(t)
          case Success((_, newRevision)) =>
            assertEquals(0, newRevision)
            done.success(())
        }
    }
  }
}

class Aggr(val state: TheOneAggr.State, val mergeEvents: Seq[AggrEvent]) {
  def appliedEvents: List[AggrEvent] = {
    val surgeon = new Surgeon(state)
    surgeon.getAll[List[AggrEvent]].head._2.reverse
  }
  private[delta] def aggr = state.curr
  def apply(cmd: AddNewNumber): Unit = {
    if (!aggr.numbers.contains(cmd.n)) {
      state(NewNumberWasAdded(cmd.n))
    }
  }
  def apply(cmd: ChangeStatus): Unit = {
    if (aggr.status != cmd.newStatus) {
      state(StatusChanged(cmd.newStatus))
    }
  }
  def numbers = aggr.numbers
}

object TheOneAggr extends Entity("", AggrStateProjector) {

  type Id = String
  type Type = Aggr

  def init(state: State, mergeEvents: List[AggrEvent]): Aggr = new Aggr(state, mergeEvents)
  def state(entity: Aggr) = entity.state
  override def validate(state: AggrState): Unit = {
    require(state.numbers.filter(_ < 0).isEmpty, "Cannot contain negative numbers")
  }
  def create(): Aggr = {
    val mutator = TheOneAggr.newState()
    mutator(new AggrCreated("New"))
    new Aggr(mutator, Nil)
  }
}

class TestEventStoreRepositoryNoSnapshots extends AbstractEventStoreRepositoryTest {

  object EvtFmt
    extends EventFormat[AggrEvent, String] {

    def getVersion(cls: EventClass) = NoVersion
    def getName(cls: EventClass): String = cls.getSimpleName

    def encode(evt: AggrEvent): String = evt match {
      case AggrCreated(status) => s""" { "status": "$status" } """
      case NewNumberWasAdded(num) => s""" { "num": $num } """
      case StatusChanged(newStatus) => s""" { "status": "$newStatus" } """
    }
    def decode(encoded: Encoded): AggrEvent = {
      val json = encoded.data
      encoded.name match {
        case "AggrCreated" => AggrCreated(status = json.field("status"))
        case "NewNumberWasAdded" => NewNumberWasAdded(n = json.field("num").toInt)
        case "StatusChanged" => StatusChanged(newStatus = json.field("status"))
      }
    }
  }

  @Before
  def setup(): Unit = {

    es = new TransientEventStore[String, AggrEvent, String](
          RandomDelayExecutionContext, EvtFmt)(_ => ticker) 
        with MessageHubPublishing[String, AggrEvent] {
      def toTopic(ch: Channel) = Topic(s"transactions/$ch")
      def toTopic(txn: TXN): Topic = toTopic(txn.channel)
      val txnHub = new LocalHub[TXN](toTopic, ec)
      val txnChannels = Set(TheOneAggr.channel)
      val txnCodec = Codec.noop[TXN]
    }
    repo = new EntityRepository(TheOneAggr, ec)(es)
  }

}

class TestEventStoreRepositoryWithSnapshots extends AbstractEventStoreRepositoryTest {

  import ReflectiveDecoder._

  object EvtFmt
    extends ReflectiveDecoder[AggrEvent, String](MatchOnMethodName)
    with AggrEventHandler
    with EventFormat[AggrEvent, String] {

    type Return = String

    def getVersion(cls: EventClass) = NoVersion
    def getName(cls: EventClass): String = {
      val lastDot = cls.getName.lastIndexOf('.')
      val nextDot = cls.getName.lastIndexOf('.', lastDot - 1)
      cls.getName.substring(nextDot + 1)
    }

    def encode(evt: AggrEvent): String = evt.dispatch(this)

    def on(evt: AggrCreated): Return = s""" { "status": "${evt.status}" } """
    def `testing.AggrCreated`(encoded: Encoded): AggrCreated =
      new AggrCreated(status = encoded.data.field("status"))

    def on(evt: NewNumberWasAdded): Return = s""" { "num": ${evt.n} } """
    def `testing.NewNumberWasAdded`(encoded: Encoded): NewNumberWasAdded =
      new NewNumberWasAdded(n = encoded.data.field("num").toInt)

    def on(evt: StatusChanged): Return = s""" { "status": "${evt.newStatus}" } """
    def `testing.StatusChanged`(encoded: Encoded): StatusChanged = new StatusChanged(newStatus = encoded.data.field("status"))
  }

  case class Metrics(id: String, duration: Long, timestamp: Long)
  var metrics: List[Metrics] = _

  @Before
  def setup(): Unit = {
    metrics = Nil
    es = new TransientEventStore[String, AggrEvent, String](
           RandomDelayExecutionContext, EvtFmt)(_ => ticker) 
         with MessageHubPublishing[String, AggrEvent] {
      def toTopic(ch: Channel) = Topic(s"transactions/$ch")
      def toTopic(txn: TXN): Topic = toTopic(txn.channel)
      val txnHub = new LocalHub[TXN](toTopic, RandomDelayExecutionContext)
      val txnChannels = Set(TheOneAggr.channel)
      val txnCodec = Codec.noop[TXN]
    }
    type Value = ConcurrentMapStore.Value[AggrState]
    val snapshotMap = new TrieMap[String, Value]
    val snapshotStore = ConcurrentMapStore[String, AggrState](snapshotMap, None)(_ => Future successful None)
    repo = new EntityRepository(TheOneAggr, ec)(es, snapshotStore)
  }
}

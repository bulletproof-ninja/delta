package ulysses.test_repo

import java.lang.reflect.Method
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.{ MILLISECONDS, SECONDS }
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Seq
import scala.concurrent.{ Future, Promise }
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.language.implicitConversions
import scala.reflect.classTag
import scala.util.{ Failure, Success, Try }
import org.bson.{ BsonReader, BsonUndefined, BsonWriter, Document }
import org.bson.codecs.{ BsonUndefinedCodec, DecoderContext, EncoderContext }
import org.junit._
import org.junit.Assert._
import com.datastax.driver.core.{ Cluster, Session }
import com.mongodb.async.client.{ MongoClients, MongoCollection }
import com.mongodb.client.result.DeleteResult
import scuff.{ Codec, DoubleDispatch, Timestamp }
import scuff.ddd.{ Repository, UnknownIdException }
import ulysses.mongo.{ MongoEventStore, UlyssesCollection, withBlockingCallback }
import ulysses.ddd.SnapshotStore
import com.mongodb.connection.ConnectionPoolSettings
import com.datastax.driver.core.QueryOptions
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.SocketOptions
import ulysses.ddd._
import ulysses.util.TransientEventStore
import ulysses.cassandra.CassandraEventStore
import ulysses.cassandra.TableDescriptor
import ulysses.util.ReflectiveEventDecoding
import scala.{ SerialVersionUID => version }
import ulysses.EventStore

trait AggrEventHandler {
  final def apply(evt: AggrEvent) = evt.dispatch(this)
}

case class AggrState(status: String, numbers: Set[Int])

final class AggrStateMutator(var state: AggrState)
    extends StateMutator[AggrEvent, AggrState]
    with AggrEventHandler {
}

sealed abstract class AggrEvent extends DoubleDispatch[AggrEventHandler]

abstract class AbstractEventStoreRepositoryTest {

  class TimestampCodec(name: String) extends Codec[Timestamp, Map[String, String]] {
    def encode(ts: Timestamp): Map[String, String] = Map(name -> ts.toString)
    def decode(map: Map[String, String]): Timestamp = Timestamp.parseISO(map(name)).get
  }

  var es: EventStore[String, AggrEvent, Unit] = _
  var repo: Repository[Aggr, String] = _

  private def doAsync(f: Promise[Any] => Unit) {
    val something = Promise[Any]
    f(something)
    Await.result(something.future, 50000.seconds) match {
      case t: Throwable => throw t
      case Failure(t) => throw t
      case _ =>
    }
  }

  def metadata: Map[String, String] = Map(
    "epochMs" -> System.currentTimeMillis.toString,
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
    val newFoo = Aggr.create("Foo")
    newFoo(AddNewNumber(-1))
    repo.insert(newFoo.id, newFoo, metadata).onComplete {
      case Failure(_) =>
        repo.exists(newFoo.id).onSuccess {
          case None => done.success("Fail on negative number")
          case Some(rev) => fail(s"Should not exist: $rev")
        }
      case Success(_) => fail("Should not accept negative numbers")
    }
  }

  @Test
  def saveNewThenUpdate = doAsync { done =>
    val newFoo = Aggr.create("Foo")
    assertEquals(1, newFoo.appliedEvents.size)
    repo.insert(newFoo.id, newFoo, metadata).onSuccess {
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
                    assertTrue(foo.concurrentEvents.contains(NewNumberWasAdded(44)))
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
                        assertTrue(foo.concurrentEvents.isEmpty)
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
    val newFoo = Aggr.create("Foo")
    newFoo(AddNewNumber(42))
    newFoo.appliedEvents match {
      case AggrCreated(_) +: NewNumberWasAdded(n) +: Nil => assertEquals(42, n)
      case _ => fail("Event sequence incorrect: " + newFoo.appliedEvents)
    }
    val update1 = repo.insert(newFoo.id, newFoo).flatMap {
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
    val baz = Aggr.create("Baz")
    repo.insert(baz.id, baz).onSuccess {
      case rev0 =>
        assertEquals(0, rev0)
        repo.insert(baz.id, baz).onSuccess {
          case rev0 =>
            assertEquals(0, rev0)
            done.success(Unit)
        }
    }
  }

  @Test
  def `concurrent update` = doAsync { done =>
    val executor = java.util.concurrent.Executors.newScheduledThreadPool(16)
    val foo = Aggr.create("Foo")
    val insFut = repo.insert(foo.id, foo, metadata)
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
    val foo = Aggr.create("Foo")
    repo.insert(foo.id, foo).onComplete {
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

class Aggr(id: String, stateMutator: AggrStateMutator, val concurrentEvents: Seq[AggrEvent]) {
  type EVT = AggrEvent
  type ID = String
  def checkInvariants() {
    require(stateMutator.state.numbers.filter(_ < 0).isEmpty, "Cannot contain negative numbers")
  }
  private[ulysses] def appliedEvents: Seq[EVT] = stateMutator.appliedEvents
  private[ulysses] def aggr = stateMutator.state
  def apply(cmd: AddNewNumber) {
    if (!aggr.numbers.contains(cmd.n)) {
      stateMutator(NewNumberWasAdded(cmd.n))
    }
  }
  def apply(cmd: ChangeStatus) {
    if (aggr.status != cmd.status) {
      stateMutator(StatusChanged(cmd.status))
    }
  }
  def numbers = aggr.numbers
}

object Aggr extends AggrRoot[Aggr, String, AggrEvent, AggrState] {
  def create(id: String): Aggr = {
    val mutator = new AggrStateMutator
    mutator(new AggrCreated("New"))
    new Aggr(id, mutator, Nil)
  }
  def apply(id: String, state: AggrState, concurrentEvents: Seq[AggrEvent]) = new Aggr(id, new AggrStateMutator(state), concurrentEvents)
  def newEvents(ar: Aggr): Seq[AggrEvent] = ar.appliedEvents
  def currState(ar: Aggr): AggrState = ar.aggr
  def checkInvariants(ar: Aggr) = ar.checkInvariants()
}

//final class AggrEventCollector(var state: AggrState = null) {
//  var appliedEvents = Vector.empty[AggrEvent]
//  def apply(evt: AggrEvent) {
//    state = new AggrStateMutator(state)(evt)
//    appliedEvents :+= evt
//  }
//}

class TestEventStoreRepositoryNoSnapshots extends AbstractEventStoreRepositoryTest {

  implicit def aggr2unit(s: AggrState): Unit = ()

  @Before
  def setup {
    es = new TransientEventStore[String, AggrEvent, Unit](global, _ => Unit)
    val esRepo = new EventStoreRepository[AggrState, String, AggrEvent, String, Unit](es, AggrStateMachine, SystemClock)
    repo = new AggrRootRepository(Aggr, esRepo)
  }

}

class TestEventStoreRepositoryWithSnapshots extends AbstractEventStoreRepositoryTest {

  implicit def aggr2unit(s: AggrState): Unit = ()

  @Before
  def setup {
    es = new TransientEventStore[String, AggrEvent, Unit](global, _ => Unit)
    val snapshotMap = new collection.concurrent.TrieMap[String, SnapshotStore[AggrState, String]#Snapshot]
    val snapshotStore = new MapSnapshotStore[Aggr, String, AggrEvent, AggrState](snapshotMap)
    val esRepo = new EventStoreRepository[AggrState, String, AggrEvent, String, Unit](es, AggrStateMachine, SystemClock, snapshotStore)
    repo = new AggrRootRepository(Aggr, esRepo)
  }

}

//@Ignore
class TestMongoEventStore extends AbstractEventStoreRepositoryTest {
  import org.bson.Document
  import ulysses.mongo._
  import com.mongodb.async.client._

  implicit def aggr2unit(s: AggrState): Unit = ()

  implicit object AggrEventCodec
      extends ReflectiveEventDecoding[AggrEvent, Document]
      with EventContext[AggrEvent, Unit, Document]
      with AggrEventHandler {
    type RT = Document
    def version(evt: Class[AggrEvent]) = scuff.serialVersionUID(evt)
    def name(evt: Class[AggrEvent]) = evt.getSimpleName
    def channel(evt: Class[AggrEvent]) = ()
    protected def evtTag = classTag[AggrEvent]
    protected def fmtTag = classTag[Document]

    def encodeEvent(evt: AggrEvent): Document = evt.dispatch(this)

    def on(evt: AggrCreated) = new Document("status", evt.status)
    def aggrCreated(version: Short, doc: Document): AggrCreated = version match {
      case 1 => AggrCreated(doc.getString("status"))
    }

    def on(evt: NewNumberWasAdded) = new Document("number", evt.n)
    def newNumberWasAdded(version: Short, doc: Document): NewNumberWasAdded = version match {
      case 1 => NewNumberWasAdded(doc.getInteger("number"))
    }

    def on(evt: StatusChanged) = new Document("newStatus", evt.newStatus)
    def statusChanged(version: Short, doc: Document): StatusChanged = version match {
      case 1 => StatusChanged(doc.getString("newStatus"))
    }
  }

  var coll: MongoCollection[Document] = _

  object UnitCodec extends org.bson.codecs.Codec[Unit] {
    private[this] val codec = new BsonUndefinedCodec
    private[this] val undefined = new BsonUndefined
    def decode(r: BsonReader, c: DecoderContext): Unit = codec.decode(r, c)
    def encode(w: BsonWriter, u: Unit, c: EncoderContext): Unit = codec.encode(w, undefined, c)
    def getEncoderClass() = classOf[Unit]
  }

  @Before
  def setup {
    val poolSettings = ConnectionPoolSettings.builder().maxWaitQueueSize(1000).build()
    val settings = MongoClientSettings.builder(MongoClients.create().getSettings).connectionPoolSettings(poolSettings).build()
    coll = MongoClients.create(settings).getDatabase("test").getCollection(getClass.getName).withCodec(UnitCodec)
    val result = deleteAll(coll)
    assertTrue(result.wasAcknowledged)
    es = new MongoEventStore[String, AggrEvent, Unit](coll)
    val esRepo = new EventStoreRepository[AggrState, String, AggrEvent, String, Unit](es, AggrStateMachine, SystemClock)
    repo = new AggrRootRepository(Aggr, esRepo)
  }
  private def deleteAll(coll: MongoCollection[_]): DeleteResult = {
    val result = withBlockingCallback[DeleteResult]() { callback =>
      coll.deleteMany(new Document, callback)
    }
    result.get
  }
  @After
  def teardown {
    val result = deleteAll(coll)
    assertTrue(result.wasAcknowledged)
  }
}

//@Ignore
class TestCassandraEventStoreRepository extends AbstractEventStoreRepositoryTest {

  implicit def aggr2unit(s: AggrState): Unit = ()

  implicit object AggrEventCodec
      extends ReflectiveEventDecoding[AggrEvent, String]
      with EventContext[AggrEvent, Unit, String]
      with AggrEventHandler {
    type RT = String
    def version(cls: Class[AggrEvent]) = scuff.serialVersionUID(cls)
    def name(cls: Class[AggrEvent]) = cls.getSimpleName
    protected def evtTag = classTag[AggrEvent]
    protected def fmtTag = classTag[String]

    def encode(evt: AggrEvent): String = evt.dispatch(this)

    def on(evt: AggrCreated) = s"""{"status":"${evt.status}"}"""
    val FindStatus = """\{"status":"(\w+)"\}""".r
    def aggrCreated(version: Short, json: String): AggrCreated = version match {
      case 1 => FindStatus.findFirstMatchIn(json).map(m => AggrCreated(m group 1)).getOrElse(throw new IllegalArgumentException(json))
    }

    def on(evt: NewNumberWasAdded) = s"""{"number":${evt.n}}"""
    val FindNumber = """\{"number":(\d+)\}""".r
    def newNumberWasAdded(version: Short, json: String): NewNumberWasAdded = version match {
      case 1 => FindNumber.findFirstMatchIn(json).map(m => NewNumberWasAdded(m.group(1).toInt)).getOrElse(throw new IllegalArgumentException(json))
    }

    def on(evt: StatusChanged) = s"""{"newStatus":"${evt.newStatus}"}"""
    val FindNewStatus = """\{"newStatus":"(\w*)"\}""".r
    def statusChanged(version: Short, json: String): StatusChanged = version match {
      case 1 => FindNewStatus.findFirstMatchIn(json).map(m => StatusChanged(m group 1)).getOrElse(throw new IllegalArgumentException(json))
    }
  }

  val Keyspace = s"${getClass.getPackage.getName}"
  val Table = getClass.getSimpleName

  object TableDescriptor extends TableDescriptor {
    def keyspace = Keyspace
    def table = Table
    val replication: Map[String, Any] = Map(
      "class" -> "SimpleStrategy",
      "replication_factor" -> 1)
  }

  var session: Session = _

  @Before
  def setup {
    session = Cluster.builder().withSocketOptions(new SocketOptions().setConnectTimeoutMillis(10000)).addContactPoints("localhost").build().connect()
    deleteAll(session)
    es = new CassandraEventStore[String, AggrEvent, Unit, String](session, TableDescriptor, 10 * 1024)
    val esRepo = new EventStoreRepository[AggrState, String, AggrEvent, String, Unit](es, AggrStateMachine, SystemClock)
    repo = new AggrRootRepository(Aggr, esRepo)
  }
  private def deleteAll(session: Session): Unit = {
    Try(session.execute(s"DROP TABLE $Keyspace.$Table;"))
  }
  @After
  def teardown {
    val result = deleteAll(session)
  }
}

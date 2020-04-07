package delta.testing

import org.junit._, Assert._
import delta.Snapshot
import scala.collection.concurrent.TrieMap
import scala.collection.compat._

import scala.concurrent.Future
import delta.process.StreamProcessStore
import delta.process.Update
import delta.process.ConcurrentMapStore
import scuff._, concurrent.ScuffFutureObject

case class TestKey(long: Long, int: Int)
case class Foo(text: String, num: Int)
object Foo extends Codec[Foo, String] {
  def encode(foo: Foo) = s"${foo.text}:${foo.num}"
  def decode(str: String): Foo = {
    val Array(text, num) = str.split(":")
    Foo(text, num.toInt)
  }
}

class TestStreamProcessStore {

  implicit val ec = RandomDelayExecutionContext

  type State = ConcurrentMapStore.State[String]

  def newStore(): StreamProcessStore[Long, String, String] =
    ConcurrentMapStore(new TrieMap[Long, State], None)(_ => Future.none)

  def newStore[S](implicit codec: Codec[S, String]): StreamProcessStore[Long, S, String] =
    newStore() adaptState codec

  protected def storeSupportsConditionalWrites: Boolean = true

  def newFooStore: StreamProcessStore[Long, Foo, String] = newStore(Foo)

  @Test
  def foo(): Unit = {
    val store = newFooStore
    val none = store.read(54L).await
    assertEquals(None, none)
    val k1 = util.Random.nextLong()
    val k2 = util.Random.nextLong()
    store.write(k1, Snapshot(Foo("Abc", 42), 5, 1111L)).await
    store.write(k2, Snapshot(Foo("Foo", 64), 8, 1110L)).await
    var rm1 = store.read(k1).await.get
    assertEquals(Snapshot(Foo("Abc", 42), 5, 1111L), rm1)
    store.refresh(k1, 9, 2222L).await
    rm1 = store.read(k1).await.get
    assertEquals(Snapshot(Foo("Abc", 42), 9, 2222L), rm1)
    store.write(k1, Snapshot(Foo("Xyz", 88888), 33, 3333L)).await
    rm1 = store.read(k1).await.get
    assertEquals(Snapshot(Foo("Xyz", 88888), 33, 3333L), rm1)
    if (storeSupportsConditionalWrites) {
      try {
        val writeSnapshot = Snapshot(Foo("Xyz", 4444), rm1.revision - 1, rm1.tick + 1)
        store.write(k1, writeSnapshot).await
        fail("Should have failed on older revision")
      } catch {
        case ise: IllegalStateException =>
          assertTrue(ise.getMessage.contains(rm1.revision.toString))
          assertTrue(ise.getMessage.contains((rm1.revision - 1).toString))
      }
      try {
        store.write(k1, Snapshot(Foo("Xyz", 4444), rm1.revision, rm1.tick - 1)).await
        fail("Should have failed on older tick")
      } catch {
        case ise: IllegalStateException =>
          assertTrue(ise.getMessage.contains(rm1.tick.toString))
          assertTrue(ise.getMessage.contains((rm1.tick - 1).toString))
      }
    }
  }

  @Test
  def `snapshot map`(): Unit = {
    val snapshot = Snapshot(98765, 123, 9999L).map(_.toString)
    assertEquals("98765", snapshot.content)
    assertEquals(123, snapshot.revision)
    assertEquals(9999L, snapshot.tick)
  }

  @Test
  def `mix of new and old`(): Unit = {
    type ID = Long
    type Tick = Long
    type Revision = Int
    type JSON = String

    val store = newStore
    val ids: Map[ID, (Revision, Tick)] = (0L until 10L).map(tick => util.Random.nextLong -> (0 -> tick)).toMap
    store.refreshBatch(ids).await // Empty store, nothing to refresh, but shouldn't fail
    val moreIds: Map[ID, (Revision, Tick)] = (20L until 30L).map(tick => util.Random.nextLong -> (0 -> tick)).toMap
    store.refreshBatch(moreIds ++ ids).await // Empty store, nothing to refresh, but shouldn't fail
    val evenMoreIds: Map[ID, (Revision, Tick)] = (40L until 50L).map(tick => util.Random.nextLong -> (0 -> tick)).toMap
    store.refreshBatch(ids ++ evenMoreIds).await // Empty store, nothing to refresh, but shouldn't fail
    val newIdSnapshots: Map[ID, Snapshot[JSON]] = (60L until 70L).map(tick => util.Random.nextLong -> Snapshot("{}", 0, tick)).toMap
    val oldIdSnapshots: Map[ID, Snapshot[JSON]] = ids.view.mapValues {
      case (rev, tick) => Snapshot("{}", rev + 1, tick + 10)
    }.toMap
    store.writeBatch(oldIdSnapshots ++ newIdSnapshots).await
    val moreNewIdSnapshots: Map[ID, Snapshot[JSON]] = (80L to 90L).map(tick => util.Random.nextLong -> Snapshot("{}", 0, tick)).toMap
    store.writeBatch(moreNewIdSnapshots ++ oldIdSnapshots).await
    val allIds = (ids ++ moreIds ++ evenMoreIds) // Only `ids` have been inserted, through oldIdSnapshots
    val all: Map[ID, Snapshot[JSON]] = store.readBatch(allIds.keys).await.toMap
    assertEquals(oldIdSnapshots.size, all.size)
    all.foreach {
      case (id, s @ Snapshot(data, rev, tick)) =>
        assertEquals("{}", data)
        assertEquals(oldIdSnapshots(id).revision, rev)
        assertEquals(oldIdSnapshots(id).tick, tick)
        println(s)
    }
  }

  @Test
  def `tick collision`(): Unit = {
    val store = newStore

    val key = util.Random.nextLong()
    val snapshot1 = new Snapshot("[1]", -1, 555L)
    store.write(key, snapshot1).await
    assertEquals(snapshot1, store.read(key).await.get)
    val snapshot2 = new Snapshot("[1,2]", -1, 555L)
    store.write(key, snapshot2).await
    assertEquals(snapshot2, store.read(key).await.get)
    val noUpdate = store.upsert(key) {
      case None =>
        fail("Should not happen"); ???
      case Some(existing) =>
        assertEquals(snapshot2, existing)
        Future successful None -> (())
    }.await._1
    assertEquals(None, noUpdate)
    assertEquals(snapshot2, store.read(key).await.get)
    val stillNoUpdate = store.upsert(key) {
      case None =>
        fail("Should not happen"); ???
      case Some(existing) =>
        assertEquals(snapshot2, existing)
        Future successful Some(Snapshot(existing.content, -1, 555L)) -> (())
    }.await._1
    assertEquals(None, stillNoUpdate)
    assertEquals(snapshot2, store.read(key).await.get)
    val update3 = store.upsert(key) {
      case None =>
        fail("Should not happen"); ???
      case Some(existing) =>
        assertEquals(snapshot2, existing)
        Future successful Some(snapshot2.copy(tick = 556L)) -> (())
    }.await._1.get
    assertTrue(update3.changed.isEmpty)
    assertEquals(Update(None, -1, 556L), update3)
    val snapshot3 = Snapshot(snapshot2.content, update3.revision, update3.tick)
    assertEquals(snapshot3, store.read(key).await.get)
    val update4 = store.upsert(key) {
      case None =>
        fail("Should not happen"); ???
      case Some(existing) =>
        assertEquals(snapshot3, existing)
        Future successful Some(Snapshot("[1,2,3]", -1, 556L)) -> (())
    }.await._1.get
    // assertTrue(update4.change.isDefined)
    assertEquals(Update(Some("[1,2,3]"), -1, 556L), update4)
    assertEquals(update4.changed.get, store.read(key).await.get.content)
  }

}

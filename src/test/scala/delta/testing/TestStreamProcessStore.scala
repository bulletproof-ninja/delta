package delta.testing

import org.junit._, Assert._
import delta.Snapshot
import scala.collection.concurrent.TrieMap
import delta.util.StreamProcessStore
import delta.util.ConcurrentMapStore
import scala.concurrent.Future

case class TestKey(long: Long, int: Int)

class TestStreamProcessStore {

  implicit val ec = RandomDelayExecutionContext

  def newStore: StreamProcessStore[Long, String] = new ConcurrentMapStore(new TrieMap, _ => Future successful None)

  @Test
  def foo() {
    val store = newStore
    val none = store.read(54L).await
    assertEquals(None, none)
    val k1 = util.Random.nextLong()
    val k2 = util.Random.nextLong()
    store.write(k1, Snapshot("Abc:42", 5, 1111L)).await
    store.write(k2, Snapshot("Foo:64", 8, 1110L)).await
    var rm1 = store.read(k1).await.get
    assertEquals(Snapshot("Abc:42", 5, 1111L), rm1)
    store.refresh(k1, 9, 2222L).await
    rm1 = store.read(k1).await.get
    assertEquals(Snapshot("Abc:42", 9, 2222L), rm1)
    store.write(k1, Snapshot("Xyz:88888", 33, 3333L)).await
    rm1 = store.read(k1).await.get
    assertEquals(Snapshot("Xyz:88888", 33, 3333L), rm1)
    try {
      val writeSnapshot = Snapshot("Xyz:4444", rm1.revision - 1, rm1.tick + 1)
      store.write(k1, writeSnapshot).await
      fail("Should have failed on older revision")
    } catch {
      case ise: IllegalStateException =>
        assertTrue(ise.getMessage.contains(rm1.revision.toString))
        assertTrue(ise.getMessage.contains((rm1.revision - 1).toString))
    }
    try {
      store.write(k1, Snapshot("Xyz:4444", rm1.revision, rm1.tick - 1)).await
      fail("Should have failed on older tick")
    } catch {
      case ise: IllegalStateException =>
        assertTrue(ise.getMessage.contains(rm1.tick.toString))
        assertTrue(ise.getMessage.contains((rm1.tick - 1).toString))
    }
  }

  @Test
  def `snapshot map`() {
    val snapshot = Snapshot(98765, 123, 9999L).map(_.toString)
    assertEquals("98765", snapshot.content)
    assertEquals(123, snapshot.revision)
    assertEquals(9999L, snapshot.tick)
  }

    @Test
  def `mix of new and old`() {
    val store = newStore
    val ids = (1L to 10L).map(tick => util.Random.nextLong -> (0 -> tick)).toMap
    store.refreshBatch(ids).await
    val moreIds = (20L to 30L).map(tick => util.Random.nextLong -> (0 -> tick)).toMap
    store.refreshBatch(moreIds ++ ids).await
    val evenMoreIds = (40L to 50L).map(tick => util.Random.nextLong -> (0 -> tick)).toMap
    store.refreshBatch(ids ++ evenMoreIds).await
    val newIdSnapshots = (60L to 70L).map(tick => util.Random.nextLong -> Snapshot("{}", 0, tick)).toMap
    val oldIdSnapshots = ids.mapValues {
      case (rev, tick) => Snapshot("{}", rev + 1, tick + 10)
    }
    store.writeBatch(oldIdSnapshots ++ newIdSnapshots).await
    val moreNewIdSnapshots = (80L to 90L).map(tick => util.Random.nextLong -> Snapshot("{}", 0, tick)).toMap
    store.writeBatch(moreNewIdSnapshots ++ oldIdSnapshots).await
    val all = store.readBatch((ids ++ moreIds ++ evenMoreIds).keys).await
    all.foreach {
      case (_, s @ Snapshot(data, _, _)) =>
        assertEquals("{}", data)
        println(s)
    }
  }

  @Test
  def `tick collision`() {
    val store = newStore
    import store.Update
    val key = util.Random.nextLong()
    val snapshot1 = new Snapshot("[1]", -1, 555L)
    store.write(key, snapshot1).await
    assertEquals(snapshot1, store.read(key).await.get)
    val snapshot2 = new Snapshot("[1,2]", -1, 555L)
    store.write(key, snapshot2).await
    assertEquals(snapshot2, store.read(key).await.get)
    val noUpdate = store.upsert(key) {
      case None => fail("Should not happen"); ???
      case Some(existing) =>
        assertEquals(snapshot2, existing)
        Future successful None -> Unit
    }.await._1
    assertEquals(None, noUpdate)
    assertEquals(snapshot2, store.read(key).await.get)
    val stillNoUpdate = store.upsert(key) {
      case None => fail("Should not happen"); ???
      case Some(existing) =>
        assertEquals(snapshot2, existing)
        Future successful Some(Snapshot(existing.content, -1, 555L)) -> Unit
    }.await._1
    assertEquals(None, stillNoUpdate)
    assertEquals(snapshot2, store.read(key).await.get)
    val Update(snapshot3, s3Updated) = store.upsert(key) {
      case None => fail("Should not happen"); ???
      case Some(existing) =>
        assertEquals(snapshot2, existing)
        Future successful Some(Snapshot(existing.content, -1, 556L)) -> Unit
    }.await._1.get
    assertFalse(s3Updated)
    assertEquals(Snapshot(snapshot2.content, -1, 556L), snapshot3)
    assertEquals(snapshot3, store.read(key).await.get)
    val Update(snapshot4, s4Updated) = store.upsert(key) {
      case None => fail("Should not happen"); ???
      case Some(existing) =>
        assertEquals(snapshot3, existing)
        Future successful Some(Snapshot("[1,2,3]", -1, 556L)) -> Unit
    }.await._1.get
    assertTrue(s4Updated)
    assertEquals(Snapshot("[1,2,3]", -1, 556L), snapshot4)
    assertEquals(snapshot4, store.read(key).await.get)
  }

}

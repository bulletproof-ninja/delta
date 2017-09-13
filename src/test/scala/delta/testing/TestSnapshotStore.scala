package delta.testing

import org.junit._, Assert._
import delta.Snapshot
import delta.SnapshotStore
import scala.collection.concurrent.TrieMap
import delta.util.ConcurrentMapSnapshotStore

case class TestKey(long: Long, int: Int)

class TestSnapshotStore {

  val store: SnapshotStore[Long, String] = new ConcurrentMapSnapshotStore(new TrieMap)

  @Test
  def foo() {
    val none = store.read(54L).await
    assertEquals(None, none)
    val k1 = 42L
    store.write(k1, Snapshot("Abc:42", 5, 1111L)).await
    store.write(65L, Snapshot("Foo:64", 8, 1110L)).await
    var rm1 = store.read(k1).await.get
    assertEquals(Snapshot("Abc:42", 5, 1111L), rm1)
    store.refresh(k1, 9, 2222L).await
    rm1 = store.read(k1).await.get
    assertEquals(Snapshot("Abc:42", 9, 2222L), rm1)
    store.write(k1, Snapshot("Xyz:88888", 33, 3333L)).await
    rm1 = store.read(k1).await.get
    assertEquals(Snapshot("Xyz:88888", 33, 3333L), rm1)
    try {
      store.write(k1, Snapshot("Xyz:4444", rm1.revision - 1, rm1.tick + 1)).await
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
}

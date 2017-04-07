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
    assertEquals(None, store.maxTick.await)
    val none = store.read(54L).await
    assertEquals(None, none)
    val k1 = 42L
    store.write(k1, Snapshot("Abc:42", 5, 1111L)).await
    assertEquals(1111L, store.maxTick.await.get)
    store.write(65L, Snapshot("Foo:64", 8, 1110L)).await
    assertEquals(1111L, store.maxTick.await.get)
    var rm1 = store.read(k1).await.get
    assertEquals(Snapshot("Abc:42", 5, 1111L), rm1)
    store.refresh(k1, 9, 2222L).await
    assertEquals(2222L, store.maxTick.await.get)
    rm1 = store.read(k1).await.get
    assertEquals(Snapshot("Abc:42", 9, 2222L), rm1)
    store.write(k1, Snapshot("Xyz:88888", 33, 3333L)).await
    assertEquals(3333L, store.maxTick.await.get)
    rm1 = store.read(k1).await.get
    assertEquals(Snapshot("Xyz:88888", 33, 3333L), rm1)
  }
}

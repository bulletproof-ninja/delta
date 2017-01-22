package delta.testing

import org.junit._, Assert._
import delta.Snapshot
import delta.SnapshotStore
import delta.util.MapSnapshotStore
import scala.collection.concurrent.TrieMap

class TestSnapshotStore {
  val store: SnapshotStore[(Long, Int), String] = new MapSnapshotStore(new TrieMap)

  @Test
  def foo {
    val none = store.get((54L, -34)).await
    assertEquals(None, none)
    val k1 = (42L, -333)
    store.set(k1, Snapshot("Abc:42", 5, 1111L)).await
    var rm1 = store.get(k1).await.get
    assertEquals(Snapshot("Abc:42", 5, 1111L), rm1)
    store.update(k1, 9, 2222L).await
    rm1 = store.get(k1).await.get
    assertEquals(Snapshot("Abc:42", 9, 2222L), rm1)
    store.set(k1, Snapshot("Xyz:88888", 33, 3333L)).await
    rm1 = store.get(k1).await.get
    assertEquals(Snapshot("Xyz:88888", 33, 3333L), rm1)
  }
}

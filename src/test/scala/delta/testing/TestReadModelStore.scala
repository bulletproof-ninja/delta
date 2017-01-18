package delta.testing

import delta.cqrs.ReadModelStore
import org.junit._, Assert._
import delta.cqrs.ReadModel

abstract class TestReadModelStore {
  val store: ReadModelStore[(String, Long, Int), String]

  @Test
  def foo {
    val none = store.get(("", 54L, -34)).await
    assertEquals(None, none)
    val k1 = ("abc", 42L, -333)
    store.set(k1, ReadModel("Abc:42", 5, 1111L)).await
    var rm1 = store.get(k1).await.get
    assertEquals(ReadModel("Abc:42", 5, 1111L), rm1)
    store.update(k1, 9, 2222L).await
    rm1 = store.get(k1).await.get
    assertEquals(ReadModel("Abc:42", 9, 2222L), rm1)
    store.set(k1, ReadModel("Xyz:88888", 33, 3333L)).await
    rm1 = store.get(k1).await.get
    assertEquals(ReadModel("Xyz:88888", 33, 3333L), rm1)
  }
}

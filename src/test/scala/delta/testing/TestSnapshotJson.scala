package delta.testing

import org.junit._, Assert._
import scuff.Codec
import org.boon.Boon
import delta.util.SnapshotJsonCodec
import delta.Snapshot

case class Foo(number: Int, string: String, bool: Boolean, obj: String = null)(val list: Array[String]) {
  override def equals(any: Any): Boolean = any match {
    case that: Foo =>
      this.number == that.number &&
        this.string == that.string &&
        this.bool == that.bool &&
        this.obj == that.obj &&
        this.list.sameElements(that.list)
    case _ => false
  }

}

class TestSnapshotJson {

  object FooCodec extends Codec[Foo, String] {
    def encode(foo: Foo): String = Boon.toJson(foo)
    def decode(json: String): Foo = Boon.fromJson(json, classOf[Foo])
  }

  @Test
  def zeros() {
    val codec = new SnapshotJsonCodec(FooCodec)
    val snapshot = Snapshot(new Foo(42, "JSON", true)(Array("hello", "world")), 0, 0)
    val json = codec encode snapshot
    //    println(json)
    assertEquals("""{"revision":0,"tick":0,"snapshot":{"number":42,"string":"JSON","bool":true,"list":["hello","world"]}}""", json)
    val snapshot2 = codec decode json
    assertEquals(snapshot, snapshot2)
  }

  @Test
  def max() {
    val codec = new SnapshotJsonCodec(FooCodec)
    val snapshot = Snapshot(new Foo(42, "JSON", false, "")(Array("hello", "world")), Int.MaxValue, Long.MaxValue)
    val json = codec encode snapshot
    //    println(json)
    assertEquals(s"""{"revision":${Int.MaxValue},"tick":${Long.MaxValue},"snapshot":{"number":42,"string":"JSON","obj":"","list":["hello","world"]}}""", json)
    val snapshot2 = codec decode json
    assertEquals(snapshot, snapshot2)
  }

}

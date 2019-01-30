package delta.testing

import org.junit._, Assert._
import scuff.Codec
import org.boon.Boon
import delta.util.json._
import delta.Snapshot
import delta.util.SnapshotUpdate

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
  def transpose(): Unit = {
    val snapshot = Snapshot(Option("Hello"), 1, 1)
    snapshot.transpose match {
      case Some(Snapshot("Hello", 1, 1)) => // as expected
      case _ => fail()
    }
  }

  @Test
  def zeros(): Unit = {
    val codec = new SnapshotCodec(FooCodec, "foo")
    val snapshot = Snapshot(new Foo(42, "JSON", true)(Array("hello", "world")), 0, 0)
    val json = codec encode snapshot
    //    println(json)
    assertEquals("""{"rev":0,"tick":0,"foo":{"number":42,"string":"JSON","bool":true,"list":["hello","world"]}}""", json)
    val snapshot2 = codec decode json
    assertEquals(snapshot, snapshot2)
  }

  @Test
  def max(): Unit = {
    val codec = new SnapshotCodec(FooCodec, "foo")
    val snapshot = Snapshot(new Foo(42, "JSON", false, "")(Array("hello", "world")), Int.MaxValue, Long.MaxValue)
    val json = codec encode snapshot
    //    println(json)
    assertEquals(s"""{"rev":${Int.MaxValue},"tick":${Long.MaxValue},"foo":{"number":42,"string":"JSON","obj":"","list":["hello","world"]}}""", json)
    val snapshot2 = codec decode json
    assertEquals(snapshot, snapshot2)
  }

  @Test
  def min(): Unit = {
    val codec = new SnapshotCodec(FooCodec, "foo")
    val snapshot = Snapshot(new Foo(42, "JSON", false, "")(Array("hello", "world")), Int.MinValue, Long.MinValue)
    val json = codec encode snapshot
    assertEquals(s"""{"tick":${Long.MinValue},"foo":{"number":42,"string":"JSON","obj":"","list":["hello","world"]}}""", json)
    val snapshot2 = codec decode json
    assertEquals(snapshot.copy(revision = -1), snapshot2)
  }

  @Test
  def snapshotUpdateTrue(): Unit = {
    val snapshotCodec = new SnapshotCodec(FooCodec, "foo")
    val snapshot = Snapshot(new Foo(42, "JSON", true)(Array("hello", "world")), 0, 0)
    val snapshotJson = snapshotCodec encode snapshot
    val updateCodec = new SnapshotUpdateCodec(snapshotCodec)
    val update999 = SnapshotUpdate(snapshot, true)
    val update999Json = updateCodec encode update999
    assertEquals(update999.contentUpdated, updateCodec.contentUpdated(update999Json))
    assertEquals(update999.snapshot.content, updateCodec.snapshotContent(update999Json))
    assertEquals(s"""{"contentUpdated":true,"snapshot":$snapshotJson}""", update999Json)
    assertTrue(updateCodec.contentUpdated(update999Json))
    assertEquals(snapshot, updateCodec.snapshot(update999Json))
    val update999_2 = updateCodec decode update999Json
    assertEquals(update999, update999_2)
  }

  @Test
  def snapshotUpdateFalse(): Unit = {
    val snapshotCodec = new SnapshotCodec(FooCodec, "foo")
    val snapshot = Snapshot(new Foo(42, "JSON", true)(Array("hello", "world")), 0, 0)
    val snapshotJson = snapshotCodec encode snapshot
    val updateCodec = new SnapshotUpdateCodec(snapshotCodec)
    val update999 = SnapshotUpdate(snapshot, false)
    val update999Json = updateCodec encode update999
    assertEquals(update999.contentUpdated, updateCodec.contentUpdated(update999Json))
    assertEquals(update999.snapshot.content, updateCodec.snapshotContent(update999Json))
    assertEquals(s"""{"contentUpdated":false,"snapshot":$snapshotJson}""", update999Json)
    assertFalse(updateCodec.contentUpdated(update999Json))
    assertEquals(snapshot, updateCodec.snapshot(update999Json))
    val update999_2 = updateCodec decode update999Json
    assertEquals(update999, update999_2)
  }

}

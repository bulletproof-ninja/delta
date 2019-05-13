package delta.testing

import org.junit._, Assert._
import scuff.Codec
import scuff.json._
import delta.util.json._
import delta.Snapshot
import delta.process.SnapshotUpdate
import scuff.json._, JsVal.DefaultConfig

case class Baz(number: Int, string: String, bool: Boolean, obj: String = null)(val list: Array[String]) {
  override def equals(any: Any): Boolean = any match {
    case that: Baz =>
      this.number == that.number &&
        this.string == that.string &&
        this.bool == that.bool &&
        this.obj == that.obj &&
        this.list.sameElements(that.list)
    case _ => false
  }

}

class TestSnapshotJson {

  object BazCodec extends Codec[Baz, String] {
    def encode(foo: Baz): String = {
      val Baz(number, string, bool, obj) = foo
      val objProp = if (obj == null) "" else s""","obj":"$obj""""
      val boolProp = if (!bool) "" else ""","bool":true"""
      val list = foo.list.map(str => s""""$str"""").mkString("[", ",", "]")
      s"""{"number":$number,"string":"$string"$boolProp$objProp,"list":$list}"""
    }
    def decode(json: String): Baz = {
      val foo = (JsVal parse json).asObj
      val obj = foo.obj getOrElse JsStr(null)
      val bool = foo.bool getOrElse JsBool.False
      Baz(
        foo.number.asNum.toInt,
        foo.string.asStr.value,
        bool.value,
        obj.value)(foo.list.asArr.map(_.asStr.value).toArray)
    }
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
    val codec = new SnapshotCodec(BazCodec, "foo")
    val snapshot = Snapshot(new Baz(42, "JSON", true)(Array("hello", "world")), 0, 0)
    val json = codec encode snapshot
    //    println(json)
    assertEquals("""{"rev":0,"tick":0,"foo":{"number":42,"string":"JSON","bool":true,"list":["hello","world"]}}""", json)
    val snapshot2 = codec decode json
    assertEquals(snapshot, snapshot2)
  }

  @Test
  def max(): Unit = {
    val codec = new SnapshotCodec(BazCodec, "foo")
    val snapshot = Snapshot(new Baz(42, "JSON", false, "")(Array("hello", "world")), Int.MaxValue, Long.MaxValue)
    val json = codec encode snapshot
    //    println(json)
    assertEquals(s"""{"rev":${Int.MaxValue},"tick":${Long.MaxValue},"foo":{"number":42,"string":"JSON","obj":"","list":["hello","world"]}}""", json)
    val snapshot2 = codec decode json
    assertEquals(snapshot, snapshot2)
  }

  @Test
  def min(): Unit = {
    val codec = new SnapshotCodec(BazCodec, "foo")
    val snapshot = Snapshot(new Baz(42, "JSON", false, "")(Array("hello", "world")), Int.MinValue, Long.MinValue)
    val json = codec encode snapshot
    assertEquals(s"""{"tick":${Long.MinValue},"foo":{"number":42,"string":"JSON","obj":"","list":["hello","world"]}}""", json)
    val snapshot2 = codec decode json
    assertEquals(snapshot.copy(revision = -1), snapshot2)
  }

  private def testSnapshotUpdate(snapshotCodec: Codec[Snapshot[Baz], String], updated: Boolean): Unit = {
    val snapshot = Snapshot(new Baz(42, "JSON", true)(Array("hello", "world")), 991, 7773)
    val updateCodec = new SnapshotUpdateCodec(snapshotCodec)
    val update999 = SnapshotUpdate(snapshot, updated)
    val update999Json = updateCodec encode update999
    val update999_2 = updateCodec decode update999Json
    assertEquals(update999, update999_2)
  }

  @Test
  def snapshotUpdate(): Unit = {
    val codec = new SnapshotCodec(BazCodec, "foo")
    testSnapshotUpdate(codec, true)
    testSnapshotUpdate(codec, false)
  }

  @Test
  def snapshotUpdateArrayCodec(): Unit = {
    val codec = new Codec[Snapshot[Baz], String] {
      def encode(snapshot: Snapshot[Baz]) = {
        s"""[${snapshot.revision},${snapshot.tick},${BazCodec encode snapshot.content}]"""
      }
      def decode(json: String): Snapshot[Baz] = {
        val arr = (JsVal parse json).asArr
        val rev = arr(0).asNum.toInt
        val tick = arr(1).asNum.toLong
        val foo = BazCodec decode arr(2).toJson
        Snapshot(foo, rev, tick)
      }
    }
    testSnapshotUpdate(codec, true)
    testSnapshotUpdate(codec, false)
  }

  @Test
  def snapshotUpdateObjCodec(): Unit = {
    val codec = new Codec[Snapshot[Baz], String] {
      def encode(snapshot: Snapshot[Baz]) = {
        s""" {"r": ${snapshot.revision}, "t": ${snapshot.tick}, "c": ${BazCodec encode snapshot.content}} """
      }
      def decode(json: String): Snapshot[Baz] = {
        val obj = (JsVal parse json).asObj
        val rev = obj.r.asNum.toInt
        val tick = obj.t.asNum.toLong
        val foo = BazCodec decode obj.c.toJson
        Snapshot(foo, rev, tick)
      }
    }
    testSnapshotUpdate(codec, true)
    testSnapshotUpdate(codec, false)
  }

}

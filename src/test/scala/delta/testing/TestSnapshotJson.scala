package delta.testing

import scuff.Codec
import scuff.json._
import delta.util.json._
import delta.Snapshot
import delta.process.Update

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

class TestSnapshotJson
extends BaseTest {

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
      val obj = foo.obj || JsStr(null)
      val bool = foo.bool || JsBool.False
      Baz(
        foo.number.asNum.toInt,
        foo.string.asStr.value,
        bool.value,
        obj.value)(foo.list.asArr.map(_.asStr.value).toArray)
    }
  }

  test("transpose") {
    val snapshot = Snapshot(Option("Hello"), 1, 1)
    snapshot.transpose match {
      case Some(Snapshot("Hello", 1, 1)) => // as expected
      case _ => fail()
    }
  }

  test("zeros") {
    val codec = new JsonSnapshot(BazCodec, "foo")
    val snapshot = Snapshot(new Baz(42, "JSON", true)(Array("hello", "world")), 0, 0)
    val json = codec encode snapshot
    //    println(json)
    assert("""{"tick":0,"revision":0,"foo":{"number":42,"string":"JSON","bool":true,"list":["hello","world"]}}""" === json)
    val snapshot2 = codec decode json
    assert(snapshot === snapshot2)
  }

  test("max") {
    val codec = new JsonSnapshot(BazCodec, "foo")
    val snapshot = Snapshot(new Baz(42, "JSON", false, "")(Array("hello", "world")), Int.MaxValue, Long.MaxValue)
    val json = codec encode snapshot
    //    println(json)
    assert(s"""{"tick":${Long.MaxValue},"revision":${Int.MaxValue},"foo":{"number":42,"string":"JSON","obj":"","list":["hello","world"]}}""" === json)
    val snapshot2 = codec decode json
    assert(snapshot === snapshot2)
  }

  test("min") {
    val codec = new JsonSnapshot(BazCodec, "foo")
    val snapshot = Snapshot(new Baz(42, "JSON", false, "")(Array("hello", "world")), Int.MinValue, Long.MinValue)
    val json = codec encode snapshot
    assert(s"""{"tick":${Long.MinValue},"foo":{"number":42,"string":"JSON","obj":"","list":["hello","world"]}}""" === json)
    val snapshot2 = codec decode json
    assert(snapshot.copy(revision = -1) === snapshot2)
  }

  private def testSnapshotUpdate(bazCodec: Codec[Baz, String], updated: Boolean): Unit = {
    val snapshot = Snapshot(new Baz(42, "JSON", true)(Array("hello", "world")), 991, 7773)
    val updateCodec = new JsonUpdate(bazCodec)
    val update999 = new Update(snapshot, updated)
    val update999Json = updateCodec encode update999
    val update999_2 = updateCodec decode update999Json
    assert(update999 === update999_2)
  }

  test("snapshotUpdate") {
    testSnapshotUpdate(BazCodec, true)
    testSnapshotUpdate(BazCodec, false)
  }

  test("snapshotUpdateArrayCodec") {
    testSnapshotUpdate(BazCodec, true)
    testSnapshotUpdate(BazCodec, false)
  }

  test("snapshotUpdateObjCodec") {
    testSnapshotUpdate(BazCodec, true)
    testSnapshotUpdate(BazCodec, false)
  }

}

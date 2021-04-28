package delta.testing

import scuff.Codec

import delta._
import delta.util.json._

import java.util.UUID
import java.time.LocalDateTime

class TestJson
extends BaseTest {

  type JSON = String

  test("transaction1") {
    val eventCodec = new Codec[String, JSON] {
      def encode(list: String) = s"""{"value":"${list.replace("\"", "\\\"")}"}"""
      def decode(json: JSON) = json.substring(10, json.length - 2).replace("\\\"", "\"")
    }
    val testEvt = eventCodec encode "ABC"
    assert("ABC" === (eventCodec decode testEvt))
    val md = Map("one" -> "1", "time" -> LocalDateTime.now.toString)
    val events = List("foo", "BAR", "", "baZZ")
    val tx = new Transaction(45623423423L, Channel("cHAnnel"), UUID.randomUUID(), 123, md, events)

    val txTransportCodec = new delta.util.json.JsonTransaction(JsonUUID, eventCodec)
    val txJson = txTransportCodec encode tx
    val restoredTx = txTransportCodec decode txJson
    assert(tx === restoredTx)
  }
  test("transaction2") {
    val eventCodec = Codec.fromString(_.toInt)
    val md = Map("one" -> "1", "time" -> LocalDateTime.now.toString)
    val events = List(1, 2, 3, Int.MaxValue)
    val tx = new Transaction(Long.MinValue, Channel(""), UUID.randomUUID(), Int.MaxValue, md, events)

    val txTransportCodec = new delta.util.json.JsonTransaction(JsonUUID, eventCodec) {
      override def streamField = "id"
      override def channelField = "ch"
      override def tickField = "@t"
      override def revisionField = "rev"
      override def metadataField = "@md"
      override def eventsField = "@ev"
    }
    val txJson = txTransportCodec encode tx
    val restoredTx = txTransportCodec decode txJson
    assert(tx === restoredTx)
  }

  test("tuples1") {
    val eventCodec = Codec.fromString(_.toInt)
    val uuid = UUID.randomUUID()
    val txTransportCodec = new JsonTransaction(JsonUUID, eventCodec)
    val codec = JsonTuple2(JsonUUID, txTransportCodec)
    val tx = Transaction.apply(23424, Channel("Hello"), uuid, 45, Map.empty, 4 :: 3 :: 2 :: 1 :: Nil)
    val tupleJson = codec.encode(uuid -> tx)
    println(tupleJson)
    val (uuid2, tx2) = codec decode tupleJson
    assert(uuid === uuid2)
    assert(tx === tx2)
  }

  test("tuples2") {
    val codec = JsonTuple2(Codec.fromString(_.toInt))
    val tupleJson = codec encode 9 -> 8
    println(tupleJson)
    val (nine, eight) = codec decode tupleJson
    assert(9 === nine)
    assert(8 === eight)
  }

  test("eventCodec") {
    val evtFmt = new EventFormat[Int, JSON] {
      def getName(cls: Class[_ <: Int]): String = "integer"
      def getVersion(cls: Class[_ <: Int]): Byte = 111
      def decode(encoded: delta.EventFormat.Encoded[String]): Int = encoded.data.toInt
      def encode(evt: Int): TestJson.this.JSON = evt.toString
    }
    val evtCodec = new JsonEvent(evtFmt)(Channel(""), Map.empty[String, String])
    val jsonMax = evtCodec encode Int.MaxValue
    assert(Int.MaxValue === (evtCodec decode jsonMax))
    val jsonMin = evtCodec encode Int.MinValue
    assert(Int.MinValue === (evtCodec decode jsonMin))
  }

  test("eventCodec_noVersion") {
    val evtFmt = new EventFormat[Int, JSON] {
      def getName(cls: Class[_ <: Int]): String = "integer"
      def getVersion(cls: Class[_ <: Int]): Byte = NotVersioned
      def decode(encoded: delta.EventFormat.Encoded[String]): Int = encoded.data.toInt
      def encode(evt: Int): TestJson.this.JSON = evt.toString
    }
    val evtCodec = new JsonEvent(evtFmt)(Channel(""), Map.empty[String, String])
    val jsonMax = evtCodec encode Int.MaxValue
    assert(Int.MaxValue === (evtCodec decode jsonMax))
    val jsonMin = evtCodec encode Int.MinValue
    assert(Int.MinValue === (evtCodec decode jsonMin))
  }

  test("jsonPatch") {

  }



}

package delta.testing

import org.junit._, Assert._
import scuff.Codec
import delta.Transaction, Transaction.Channel
import java.util.UUID
import java.time.LocalDateTime
import delta.util.json._
import delta.EventFormat

class TestJson {
  type JSON = String

  @Test
  def transaction1() = {
    val eventCodec = new Codec[String, JSON] {
      def encode(list: String) = s"""{"value":"${list.replace("\"", "\\\"")}"}"""
      def decode(json: JSON) = json.substring(10, json.length - 2).replace("\\\"", "\"")
    }
    val testEvt = eventCodec encode "ABC"
    assertEquals("ABC", eventCodec decode testEvt)
    val md = Map("one" -> "1", "prison" -> "\\\"escape \\uffbb\\\"", "time" -> LocalDateTime.now.toString)
    val events = List("foo", "BAR", "", "baZZ")
    val txn = new Transaction(45623423423L, Channel("cHAnnel"), UUID.randomUUID(), 123, md, events)

    val txnCodec = new delta.util.json.TransactionCodec(UUIDCodec, eventCodec)
    val txnJson = txnCodec encode txn
    val restoredTxn = txnCodec decode txnJson
    assertEquals(txn, restoredTxn)
  }
  @Test
  def transaction2() = {
    val eventCodec = Codec.fromString(_.toInt)
    val md = Map("one" -> "1", "prison" -> "\\\"escape \\uffbb\\\"", "time" -> LocalDateTime.now.toString)
    val events = List(1, 2, 3, Int.MaxValue)
    val txn = new Transaction(Long.MinValue, Channel(""), UUID.randomUUID(), Int.MaxValue, md, events)

    val txnCodec = new delta.util.json.TransactionCodec(UUIDCodec, eventCodec) {
      override def streamField = "id"
      override def channelField = "ch"
      override def tickField = "@t"
      override def revisionField = "rev"
      override def metadataField = "@md"
      override def eventsField = "@ev"
    }
    val txnJson = txnCodec encode txn
    val restoredTxn = txnCodec decode txnJson
    assertEquals(txn, restoredTxn)
  }

  @Test
  def tuples1() = {
    val eventCodec = Codec.fromString(_.toInt)
    val uuid = UUID.randomUUID()
    val txnCodec = new TransactionCodec(UUIDCodec, eventCodec)
    val codec = Tuple2Codec(UUIDCodec, txnCodec)
    val txn = Transaction.apply(23424, Transaction.Channel("Hello"), uuid, 45, Map.empty, 4 :: 3 :: 2 :: 1 :: Nil)
    val tupleJson = codec encode uuid -> txn
    println(tupleJson)
    val (uuid2, txn2) = codec decode tupleJson
    assertEquals(uuid, uuid2)
    assertEquals(txn, txn2)
  }

  @Test
  def tuples2() = {
    val codec = Tuple2Codec(Codec.fromString(_.toInt))
    val tupleJson = codec encode 9 -> 8
    println(tupleJson)
    val (nine, eight) = codec decode tupleJson
    assertEquals(9, nine)
    assertEquals(8, eight)
  }

  @Test
  def eventCodec() = {
    val evtFmt = new EventFormat[Int, JSON] {
      def getName(cls: Class[_ <: Int]): String = "integer"
      def getVersion(cls: Class[_ <: Int]): Byte = 111
      def decode(encoded: delta.EventFormat.Encoded[String]): Int = encoded.data.toInt
      def encode(evt: Int): TestJson.this.JSON = evt.toString
    }
    val evtCodec = new EventCodec(evtFmt)(Channel(""), Map.empty[String, String])
    val jsonMax = evtCodec encode Int.MaxValue
    assertEquals(Int.MaxValue, evtCodec decode jsonMax)
    val jsonMin = evtCodec encode Int.MinValue
    assertEquals(Int.MinValue, evtCodec decode jsonMin)
  }

  @Test
  def eventCodec_noVersion() = {
    val evtFmt = new EventFormat[Int, JSON] {
      def getName(cls: Class[_ <: Int]): String = "integer"
      def getVersion(cls: Class[_ <: Int]): Byte = NoVersion
      def decode(encoded: delta.EventFormat.Encoded[String]): Int = encoded.data.toInt
      def encode(evt: Int): TestJson.this.JSON = evt.toString
    }
    val evtCodec = new EventCodec(evtFmt)(Channel(""), Map.empty[String, String])
    val jsonMax = evtCodec encode Int.MaxValue
    assertEquals(Int.MaxValue, evtCodec decode jsonMax)
    val jsonMin = evtCodec encode Int.MinValue
    assertEquals(Int.MinValue, evtCodec decode jsonMin)
  }
}

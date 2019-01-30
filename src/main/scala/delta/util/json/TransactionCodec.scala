package delta.util.json

import java.lang.StringBuilder

import scuff.Codec
import delta.Transaction, Transaction.Channel
import scuff.Numbers
import delta.EventFormat
import scala.annotation.tailrec

/**
 * @tparam ID Id type
 * @tparam EVT Event type
 */
class TransactionCodec[ID, EVT](jsonIdCodec: Codec[ID, JSON], jsonEventCodec: (Transaction.Channel, Map[String, String]) => Codec[EVT, JSON])
  extends Codec[Transaction[ID, EVT], JSON] {

  def this(jsonIdCodec: Codec[ID, JSON], jsonEventFormat: EventFormat[EVT, JSON]) =
    this(jsonIdCodec, new EventCodec(jsonEventFormat)(_, _))

  def this(jsonIdCodec: Codec[ID, JSON], jsonEventCodec: Codec[EVT, JSON]) =
    this(jsonIdCodec, (_, _) => jsonEventCodec)

  protected def tickField: String = "tick"
  protected def channelField: String = "channel"
  protected def streamField: String = "stream"
  protected def revisionField: String = "revision"
  protected def metadataField: String = "metadata"
  protected def eventsField: String = "events"

  private[this] val tickFieldLength = /* {" */ 2 + tickField.length + /* ": */ 2
  private[this] val channelPropStart = s"""","$channelField":"""
  private[this] val streamPropStart = s"""","$streamField":"""
  private[this] val revisionPropStart = s""","$revisionField":"""
  private[this] val metadataPropStart = s""","$metadataField":"""
  private[this] val eventsPropStart = s""","$eventsField":"""

  private def parseTick(json: JSON): Long = Numbers.parseUnsafeLong(json, tickFieldLength)(stop = Comma)
  private def channelDataOffset(tickLength: Int): Int =
    tickFieldLength + tickLength + channelPropStart.length
  private def channelDataLength(json: JSON, channelDataOffset: Int): Int =
    json.indexOf(streamPropStart, channelDataOffset) - channelDataOffset

  private def streamDataOffset(channelDataOffset: Int, channelDataLength: Int): Int =
    channelDataOffset + channelDataLength + streamPropStart.length

  private def streamDataLength(json: JSON, streamDataOffset: Int): Int =
    json.indexOf(revisionPropStart, streamDataOffset) - streamDataOffset

  private def revisionDataOffset(streamDataOffset: Int, streamDataLength: Int): Int =
    streamDataOffset + streamDataLength + revisionPropStart.length

  private def parseRevision(json: JSON, revisionDataOffset: Int): Int = Numbers.parseUnsafeInt(json, revisionDataOffset)(stop = Comma)

  private def metadataDataOffset(revDataOffset: Int, revDataLength: Int): Int =
    revDataOffset + revDataLength + metadataPropStart.length

  private def parseMetadata(json: JSON, metadataDataOffset: Int): (Map[String, String], Int) = {
      def parseString(pos: Int, sb: StringBuilder, escaped: Boolean): String = {
        if (escaped) parseString(pos + 1, sb append json.charAt(pos), false)
        else json.charAt(pos) match {
          case '"' =>
            val str = sb.toString()
            sb.setLength(0)
            str
          case '\\' => parseString(pos + 1, sb append '\\', true)
          case c => parseString(pos + 1, sb append c, false)
        }
      }
      def parse(pos: Int, sb: StringBuilder, map: Map[String, String]): (Map[String, String], Int) = {
        json.charAt(pos) match {
          case '{' | ',' => parse(pos + 1, sb, map)
          case '}' => map -> (pos + 1)
          case '"' =>
            val key = parseString(pos + 1, sb, false)
            val value = parseString(pos + key.length + 4, sb, false)
            parse(pos + key.length + 4 + value.length + 1, sb, map.updated(key, value))
        }
      }
    parse(metadataDataOffset, new StringBuilder(128), Map.empty)
  }

  def encode(txn: Transaction[ID, EVT]): JSON = {
    val streamJson: JSON = jsonIdCodec.encode(txn.stream)
    val mdJson = txn.metadata.map {
      case (key, value) => s""""$key":"$value""""
    }.mkString("{", ",", "}")
    val codec = jsonEventCodec(txn.channel, txn.metadata)
    val eventsJson: JSON = txn.events.map(codec.encode).mkString("[", ",", "]")
    s"""{"$tickField":${txn.tick},"$channelField":"${txn.channel}","$streamField":$streamJson,"$revisionField":${txn.revision},"$metadataField":$mdJson,"$eventsField":$eventsJson}"""
  }

  def decode(json: JSON): Transaction[ID, EVT] = {
    val tick = parseTick(json)
    val tickLen = numLength(tick)

    val chOffset = channelDataOffset(tickLen)
    val chLen = channelDataLength(json, chOffset)
    val channel = Transaction.Channel(json.substring(chOffset, chOffset + chLen))

    val streamOffset = streamDataOffset(chOffset, chLen)
    val streamLen = streamDataLength(json, streamOffset)
    val streamJson = json.substring(streamOffset, streamOffset + streamLen)
    val streamId = jsonIdCodec decode streamJson

    val revOffset = revisionDataOffset(streamOffset, streamLen)
    val revision = parseRevision(json, revOffset)
    val revLen = numLength(revision)

    val mdOffset = metadataDataOffset(revOffset, revLen)
    val (metadata, endIdx) = parseMetadata(json, mdOffset)

    val evtCodec = jsonEventCodec(channel, metadata)
    val eventsJson = json.substring(endIdx + eventsPropStart.length + 1, json.length - 2) // Also remove array brackets
    val events = parseCommaSeparatedEvents(evtCodec.decode, eventsJson)

    new Transaction(tick, channel, streamId, revision, metadata, events)
  }

  @tailrec
  private def parseCommaSeparatedEvents(decoder: JSON => EVT, json: JSON, pos: Int = 0, currStartPos: Int = 0, depth: Int = 0, inString: Boolean = false, events: List[EVT] = Nil): List[EVT] = {
      def prependEvent(): List[EVT] = {
        val singleEventJson = json.substring(currStartPos, pos)
        decoder(singleEventJson) :: events
      }
    if (pos == json.length) {
      if (inString || depth != 0) throw new IllegalArgumentException(s"Invalid JSON: $json")
      prependEvent().reverse
    } else if (inString) {
      json.charAt(pos) match {
        case '"' =>
          parseCommaSeparatedEvents(decoder, json, pos + 1, currStartPos, depth, inString = false, events)
        case '\\' =>
          parseCommaSeparatedEvents(decoder, json, pos + 2, currStartPos, depth, inString, events)
        case _ =>
          parseCommaSeparatedEvents(decoder, json, pos + 1, currStartPos, depth, inString, events)
      }
    } else { // not inString
      json.charAt(pos) match {
        case ',' if depth == 0 =>
          parseCommaSeparatedEvents(decoder, json, pos + 1, pos + 1, depth, inString, prependEvent())
        case '"' =>
          parseCommaSeparatedEvents(decoder, json, pos + 1, currStartPos, depth, inString = true, events)
        case '{' | '[' =>
          parseCommaSeparatedEvents(decoder, json, pos + 1, currStartPos, depth + 1, inString, events)
        case '}' | ']' =>
          parseCommaSeparatedEvents(decoder, json, pos + 1, currStartPos, depth - 1, inString, events)
        case _ =>
          parseCommaSeparatedEvents(decoder, json, pos + 1, currStartPos, depth, inString, events)
      }
    }
  }

}

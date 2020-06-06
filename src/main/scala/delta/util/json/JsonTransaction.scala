package delta.util.json

import scuff.Codec
import scuff.json._, JsVal._

import delta._

/**
 * @tparam ID Id type
 * @tparam EVT Event type
 */
class JsonTransaction[ID, EVT](jsonIdCodec: Codec[ID, JSON], jsonEventCodec: (Channel, Map[String, String]) => Codec[EVT, JSON])
  extends Codec[Transaction[ID, EVT], JSON] {

  def this(jsonIdCodec: Codec[ID, JSON], jsonEventFormat: EventFormat[EVT, JSON]) =
    this(jsonIdCodec, new JsonEvent(jsonEventFormat)(_, _))

  def this(jsonIdCodec: Codec[ID, JSON], jsonEventCodec: Codec[EVT, JSON]) =
    this(jsonIdCodec, (_, _) => jsonEventCodec)

  protected def tickField: String = "tick"
  protected def channelField: String = "channel"
  protected def streamField: String = "stream"
  protected def revisionField: String = "revision"
  protected def metadataField: String = "metadata"
  protected def eventsField: String = "events"

  def encode(tx: Transaction[ID, EVT]): JSON = {
    val streamJson: JSON = jsonIdCodec.encode(tx.stream)
    val mdJson = tx.metadata.map {
      case (key, value) => s""""$key":"$value""""
    }.mkString("{", ",", "}")
    val codec = jsonEventCodec(tx.channel, tx.metadata)
    val eventsJson: JSON = tx.events.map(codec.encode).mkString("[", ",", "]")
    s"""{"$tickField":${tx.tick},"$channelField":"${tx.channel}","$streamField":$streamJson,"$revisionField":${tx.revision},"$metadataField":$mdJson,"$eventsField":$eventsJson}"""
  }

  def decode(json: JSON): Transaction[ID, EVT] = {
    val jsTx = (JsVal parse json).asObj
    val tick = jsTx(tickField).asNum.toLong
    val channel = Channel(jsTx(channelField).asStr.value)
    val streamId = jsonIdCodec.decode(jsTx(streamField).toJson)
    val revision = jsTx(revisionField).asNum.toInt
    val metadata = jsTx(metadataField).asObj.props.map(e => e._1 -> e._2.asStr.value)
    val events = jsTx(eventsField).asArr.values
      .iterator.map { evt =>
        jsonEventCodec(channel, metadata) decode evt.toJson
      }.toList
    new Transaction(tick, channel, streamId, revision, metadata, events)
  }

}

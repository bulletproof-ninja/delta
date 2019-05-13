package delta.util.json

import scuff.Codec
import scuff.json._, JsVal._
import delta.Transaction
import delta.EventFormat

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
    val jsTxn = (JsVal parse json).asObj
    val tick = jsTxn(tickField).asNum.toLong
    val channel = Transaction.Channel(jsTxn(channelField).asStr.value)
    val streamId = jsonIdCodec.decode(jsTxn(streamField).toJson)
    val revision = jsTxn(revisionField).asNum.toInt
    val metadata = jsTxn(metadataField).asObj.props.map(e => e._1 -> e._2.asStr.value)
    val events = jsTxn(eventsField).asArr.values
      .iterator.map { evt =>
        jsonEventCodec(channel, metadata) decode evt.toJson
      }.toList
    new Transaction(tick, channel, streamId, revision, metadata, events)
  }

}

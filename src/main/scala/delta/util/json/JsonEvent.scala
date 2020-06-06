package delta.util.json

import delta._, EventFormat.Encoded

import scuff.Codec
import scuff.json._, JsVal._

/**
 * Generic event/JSON codec, leveraging any existing `EventFormat` instance.
 * @note While this class requires `channel` and `metadata` per instance, and
 * is thus meant to be created *per* transaction, it's only necessary if the
 * supplied `EventFormat` need any of those. Otherwise it's fine to re-use an instance.
 */
class JsonEvent[EVT, EF](evtFmt: EventFormat[EVT, EF])(
    channel: Channel, metadata: Map[String, String])(
    implicit
    toJson: EF => JSON, fromJson: JSON => EF)
  extends Codec[EVT, JSON] {

  def this(evtFmt: EventFormat[EVT, EF], jsonCodec: Codec[EF, JSON])(channel: Channel, metadata: Map[String, String]) =
    this(evtFmt)(channel, metadata)(jsonCodec.encode, jsonCodec.decode)

  def this(jsonCodec: Codec[EF, JSON])(
      channel: Channel, metadata: Map[String, String])(
      implicit
      evtFmt: EventFormat[EVT, EF]) =
    this(evtFmt)(channel, metadata)(jsonCodec.encode, jsonCodec.decode)

  protected def nameField: String = "event"
  protected def versionField: String = "version"
  protected def dataField: String = "data"

  def encode(evt: EVT): JSON = {
    val evtData: JSON = evtFmt encode evt
    (evtFmt signature evt) match {
      case EventFormat.EventSig(evtName, EventFormat.NoVersion) =>
        s"""{"$nameField":"$evtName","$dataField":$evtData}"""
      case EventFormat.EventSig(evtName, evtVersion) =>
        s"""{"$nameField":"$evtName","$versionField":$evtVersion,"$dataField":$evtData}"""
    }
  }
  def decode(json: JSON): EVT = {
    val jsObj = (JsVal parse json).asObj
    val evtName = jsObj(nameField).asStr.value
    val evtVersion = jsObj(versionField) match {
      case JsNum(num) => num.byteValue
      case JsUndefined => EventFormat.NoVersion
      case _ => ???
    }
    val evtData = jsObj(dataField)
    evtFmt decode new Encoded[EF](evtName, evtVersion, evtData.toJson, channel, metadata)
  }
}

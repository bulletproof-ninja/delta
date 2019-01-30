package delta.util.json

import delta.EventFormat, EventFormat.Encoded
import delta.Transaction
import scuff.Codec
import scuff.Numbers

/**
 * Generic event/JSON codec, leveraging any existing `EventFormat` instance.
 * NOTE: While this class requires `channel` and `metadata` per instance, and
 * is thus meant to be created *per* transaction, it's only necessary if the
 * supplied `EventFormat` any of those. Otherwise it's fine to re-use an instance.
 */
class EventCodec[EVT](evtFmt: EventFormat[EVT, JSON])(channel: Transaction.Channel, metadata: Map[String, String])
  extends Codec[EVT, JSON] {

  private[this] val NamePrefix = s"""{"$nameField":""""
  private[this] val VersionPrefix = s"""","$versionField":"""
  private[this] val DataPrefix = s""","$dataField":"""

  protected def nameField: String = "event"
  protected def versionField: String = "version"
  protected def dataField: String = "data"

  def encode(evt: EVT): JSON = {
    val evtData = evtFmt encode evt
    (evtFmt signature evt) match {
      case EventFormat.EventSig(evtName, EventFormat.NoVersion) =>
        s"""{"$nameField":"$evtName","$dataField":$evtData}"""
      case EventFormat.EventSig(evtName, evtVersion) =>
        s"""{"$nameField":"$evtName","$versionField":$evtVersion,"$dataField":$evtData}"""
    }
  }
  def decode(json: JSON): EVT = {
    val (evtVersion, versionPadding, nameEndIdx) =
      json.indexOf(VersionPrefix, 6) match {
        case -1 =>
          (evtFmt.NoVersion, 1, json.indexOf(DataPrefix, 6) - 1)
        case idx =>
          val ver = Numbers.parseUnsafeInt(json, idx + VersionPrefix.length)(stop = Comma)
          (ver.toByte, VersionPrefix.length + numLength(ver), idx)
      }
    val evtName = json.substring(NamePrefix.length, nameEndIdx)
    val evtData = json.substring(nameEndIdx + versionPadding + DataPrefix.length, json.length-1)
    evtFmt decode new Encoded(evtName, evtVersion, evtData, channel, metadata)
  }
}

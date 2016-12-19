package ulysses.util

import scuff.Codec
import ulysses.Transaction
import ulysses.EventCodec

/**
  * Convenience trait for publishing
  * Transactions in JSON.
  */
trait TransactionJsonCodec[ID, EVT, CH] extends Codec[Transaction[ID, EVT, CH], String] {
  protected def channelValue(ch: CH): String
  protected def streamValue(stream: ID): String
  protected def eventCodec: EventCodec[EVT, String]
  protected def tickField = "tick"
  protected def channelField = "channel"
  protected def streamField = "stream"
  protected def revisionField = "revision"
  protected def metadataField = "metadata"
  protected def eventsField = "events"
  protected def eventNameField = "name"
  protected def eventVersionField = "version"
  protected def eventDataField = "data"
  protected def includeMetadataIfEmpty = true

  type TXN = Transaction[ID, EVT, CH]

  def encode(txn: TXN): String = {
    import txn._

    val jsonMD = metadata.map {
      case (key, value) => s""""$key":"$value""""
    }.mkString("{", ",", "}")
    val jsonEvents = events.map { evt =>
      s"""{"$eventNameField":"${eventCodec name evt}","$eventVersionField":${eventCodec version evt},"$eventDataField":${eventCodec encode evt}}"""
    }.mkString("[", ",", "]")
    val prelim = s""""$tickField":$tick,"$channelField":${channelValue(channel)},"$streamField":${streamValue(stream)},"$revisionField":$revision,"$eventsField":$jsonEvents"""
    if (includeMetadataIfEmpty || metadata.nonEmpty) {
      s"""{$prelim,"$metadataField":$jsonMD}"""
    } else {
      s"""{$prelim}"""
    }
  }
}

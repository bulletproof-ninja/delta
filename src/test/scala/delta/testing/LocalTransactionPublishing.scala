package delta.testing

import delta.MessageTransportPublishing
import delta.util.LocalTransport
import scuff.Codec

trait LocalTransactionPublishing[SID, EVT]
extends MessageTransportPublishing[SID, EVT] {
  def toTopic(ch: Channel) = Topic(s"transactions/$ch")
  def toTopic(tx: Transaction): Topic = toTopic(tx.channel)
  val txTransport = new LocalTransport[Transaction](toTopic, RandomDelayExecutionContext)
  val txCodec = Codec.noop[Transaction]
}

package ulysses

sealed abstract class StreamFilter[ID, EVT, CH] {
  type TXN = Transaction[ID, EVT, CH]
  def allowed(txn: Transaction[ID, EVT, CH]): Boolean
}

object StreamFilter {

  type C[EVT] = Class[_ <: EVT]

  def apply[ID, EVT, CH](one: C[EVT], others: C[EVT]*): StreamFilter[ID, EVT, CH] =
    new ByEvent((one +: others).toSet)
  def apply[ID, EVT, CH](one: CH, others: CH*): StreamFilter[ID, EVT, CH] =
    new ByChannel((one +: others).toSet)
  def apply[ID, EVT, CH](stream: ID, channel: CH): StreamFilter[ID, EVT, CH] =
    new ByStream(stream, channel)

  case class Everything[ID, EVT, CH]() extends StreamFilter[ID, EVT, CH] {
    def allowed(txn: TXN) = true
  }
  case class ByChannel[ID, EVT, CH](channels: Set[CH]) extends StreamFilter[ID, EVT, CH] {
    require(channels.nonEmpty)
    def allowed(txn: TXN) = channels.contains(txn.channel)
  }
  case class ByEvent[ID, EVT, CH](evtTypes: Set[C[EVT]])
      extends StreamFilter[ID, EVT, CH] {
    require(evtTypes.nonEmpty)
    def allowed(txn: TXN) = txn.events.exists(evt => evtTypes.contains(evt.getClass))
  }
  case class ByStream[ID, EVT, CH](stream: ID, channel: CH) extends StreamFilter[ID, EVT, CH] {
    def allowed(txn: TXN) = txn.stream == stream && txn.channel == channel
  }
}

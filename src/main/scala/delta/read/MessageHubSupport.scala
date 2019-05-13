package delta.read

import scuff.Subscription
import scuff.Codec

trait MessageHubSupport[ID, MHID, S]
  extends SubscriptionSupport[ID, S] {
  rm: BasicReadModel[ID, S] =>

  protected type Msg = (MHID, SnapshotUpdate)
  protected type Topic = delta.MessageHub.Topic
  protected def Topic(name: String) = delta.MessageHub.Topic(name)

  protected val snapshotHub: delta.MessageHub
  protected def snapshotTopic: Topic
  protected def idConv(id: ID): MHID
  protected def hubCodec: Codec[Msg, snapshotHub.MsgType]
  protected implicit lazy val encoder = hubCodec.encode _
  private[this] implicit lazy val decoder = hubCodec.decode _

  protected def subscribe(id: ID)(pf: PartialFunction[SnapshotUpdate, Unit]): Subscription = {
    val MatchId: MHID = idConv(id)
    snapshotHub.subscribe[Msg](snapshotTopic) {
      case (MatchId, update) if (pf isDefinedAt update) => pf(update)
    }
  }

}

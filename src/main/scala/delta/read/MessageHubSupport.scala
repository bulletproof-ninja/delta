package delta.read

import scuff.Subscription
import scuff.Codec
import delta.MessageHub

trait MessageHubSupport[ID, S, U]
extends SubscriptionSupport[ID, S, U] {
  rm: BasicReadModel[ID, S] =>

  protected type Topic = MessageHub.Topic
  protected def Topic(name: String) = MessageHub.Topic(name)

  protected val hub: MessageHub
  protected def hubTopic: Topic
  protected def hubCodec: Codec[(ID, Update), hub.Message]
  protected implicit lazy val encoder = hubCodec.encode _
  private[this] implicit lazy val decoder = hubCodec.decode _

  protected def subscribe(MatchId: ID)(pf: PartialFunction[Update, Unit]): Subscription =
    hub.subscribe[(ID, Update)](hubTopic) {
      case (MatchId, update) if (pf isDefinedAt update) => pf(update)
    }

}

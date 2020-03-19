package delta.read

import scuff.Subscription
import delta.MessageHub

trait MessageHubSupport[ID, S, U]
extends SubscriptionSupport[ID, S, U] {
  rm: BasicReadModel[ID, S] =>

  protected def hub: MessageHub[StreamId, Update]

  protected def subscribe(id: ID)(callback: Update => Unit): Subscription =
    hub.subscribe(StreamId(id))(callback)

}

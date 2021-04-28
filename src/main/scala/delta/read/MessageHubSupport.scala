package delta.read

import scuff.Subscription
import delta.{ MessageSource, MessageHub }

trait MessageSourceSupport[ID, S, U]
extends SubscriptionSupport[ID, S, U] {
  rm: ReadModel[ID, S] =>

  protected def updateSource: MessageSource[StreamId, Update]

  protected def subscribe(
      id: ID)(
      callback: Update => Unit): Subscription =
    updateSource.subscribe(StreamId(id))(callback)

}

trait MessageHubSupport[ID, S, U]
extends MessageSourceSupport[ID, S, U] {
  rm: ReadModel[ID, S] =>

  protected def updateHub: MessageHub[StreamId, Update]

  protected def updateSource: MessageSource[StreamId, Update] = updateHub

}

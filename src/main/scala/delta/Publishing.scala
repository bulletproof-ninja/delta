package delta

import scala.concurrent.Future
import scuff.Subscription

/**
  * Enable pub/sub of transactions.
  */
trait Publishing[ID, EVT]
  extends EventStore[ID, EVT] {

  protected def publisher: Publisher[ID, EVT]

  abstract final override def commit(
      channel: String, stream: ID, revision: Int, tick: Long,
      events: List[EVT], metadata: Map[String, String]): Future[TXN] = {
    val txn = super.commit(channel, stream, revision, tick, events, metadata)
    publisher.publish(txn)
    txn
  }

  override def subscribe[U](selector: CompleteSelector)(callback: TXN => U): Subscription =
    publisher.subscribe(selector.include, callback, selector.channelSubset)

}

package delta

import scala.concurrent.Future
import scuff.Subscription

/**
  * Enable pub/sub of transactions.
  */
trait Publishing[ID, EVT, CH]
  extends EventStore[ID, EVT, CH] {

  protected def publisher: Publisher[ID, EVT, CH]

  abstract final override def commit(
      channel: CH, stream: ID, revision: Int, tick: Long,
      events: List[EVT], metadata: Map[String, String]): Future[TXN] = {
    val txn = super.commit(channel, stream, revision, tick, events, metadata)
    publisher.publish(txn)
    txn
  }

  def subscribe[U](selector: CompleteSelector)(callback: TXN => U): Subscription =
    publisher.subscribe(selector.include, callback, selector.channelSubset)

}

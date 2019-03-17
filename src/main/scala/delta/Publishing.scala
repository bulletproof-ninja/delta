package delta

import scala.concurrent.Future
import scuff.Subscription

/**
 * Enable pub/sub of transactions.
 */
trait Publishing[ID, EVT]
  extends EventStore[ID, EVT] {

  protected type Namespace = MessageHub.Namespace
  protected def Namespace(str: String) = MessageHub.Namespace(str)
  protected def toNamespace(ch: Channel): Namespace
  protected def txnHub: MessageHub[TXN]
  protected def txnChannels: Set[Channel]

  abstract final override def commit(
      channel: Channel, stream: ID, revision: Int, tick: Long,
      events: List[EVT], metadata: Map[String, String]): Future[TXN] = {
    val txn = super.commit(channel, stream, revision, tick, events, metadata)
    txnHub.publish(toNamespace(channel), txn)
    txn
  }

  override def subscribe[U](selector: StreamsSelector)(callback: TXN => U): Subscription = {

    val pfCallback = new PartialFunction[TXN, U] {
      def isDefinedAt(txn: TXN) = true
      def apply(txn: TXN) = callback(txn)
    }

    val channels = {
      val channelSubset = selector.channelSubset
      if (channelSubset.isEmpty) txnChannels else channelSubset
    }

    txnHub.subscribe[U](channels.map(toNamespace))(pfCallback)
  }

}

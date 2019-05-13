package delta

import scala.concurrent.Future
import scuff.Subscription
import scuff.Codec

/**
 * Enable pub/sub of transactions.
 */
trait MessageHubPublishing[ID, EVT]
  extends EventStore[ID, EVT] {

  protected val txnHub: MessageHub
  protected def txnChannels: Set[Channel]
  protected def toTopic(ch: Channel): Topic
  protected def txnCodec: Codec[TXN, txnHub.MsgType]

  implicit private lazy val encoder = txnCodec.encode _
  implicit private lazy val decoder = txnCodec.decode _

  protected type Topic = MessageHub.Topic
  protected def Topic(name: String): Topic = MessageHub.Topic(name)

  abstract final override def commit(
      channel: Channel, stream: ID, revision: Int, tick: Long,
      events: List[EVT], metadata: Map[String, String]): Future[TXN] = {
    val txn = super.commit(channel, stream, revision, tick, events, metadata)
    txnHub.publish(toTopic(channel), txn)
    txn
  }

  override def subscribe[U](selector: StreamsSelector)(callback: TXN => U): Subscription = {

    val pfCallback = new PartialFunction[TXN, Unit] {
      def isDefinedAt(txn: TXN) = true
      def apply(txn: TXN) = callback(txn)
    }

    val channels = {
      val channelSubset = selector.channelSubset
      if (channelSubset.isEmpty) txnChannels else channelSubset
    }

    txnHub.subscribe[TXN](channels.map(toTopic))(pfCallback)
  }

}

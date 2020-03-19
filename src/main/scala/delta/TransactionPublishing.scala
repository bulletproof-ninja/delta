package delta

import scala.concurrent.Future
import scuff.Subscription
import scuff.Codec

trait TransactionPublishing[ID, EVT]
extends EventStore[ID, EVT] {

  protected def publishTransaction(stream: ID, ch: Channel, tx: Future[Transaction]): Unit

  abstract final override def commit(
      channel: Channel, stream: ID, revision: Int, tick: Long,
      events: List[EVT], metadata: Map[String, String]): Future[Transaction] = {
    val tx = super.commit(channel, stream, revision, tick, events, metadata)
    publishTransaction(stream, channel, tx)
    tx
  }

  override def subscribe[U](selector: StreamsSelector)(callback: Transaction => U): Subscription

}

/**
 * Enable pub/sub of transactions.
 */
trait MessageTransportPublishing[ID, EVT]
extends TransactionPublishing[ID, EVT] {

  protected val txTransport: MessageTransport
  protected def txChannels: Set[Channel]
  protected def toTopic(ch: Channel): Topic
  protected def txCodec: Codec[Transaction, txTransport.TransportType]

  implicit private lazy val encoder = txCodec.encode _
  implicit private lazy val decoder = txCodec.decode _

  protected def publishTransaction(stream: ID, ch: Channel, tx: Future[Transaction]): Unit =
    txTransport.publish(toTopic(ch), tx)

  protected type Topic = MessageTransport.Topic
  protected def Topic(name: String): Topic = MessageTransport.Topic(name)

  override def subscribe[U](selector: StreamsSelector)(callback: Transaction => U): Subscription = {

    val pfCallback = new PartialFunction[Transaction, Unit] {
      def isDefinedAt(tx: Transaction) = true
      def apply(tx: Transaction) = callback(tx)
    }

    val channels = {
      val channelSubset = selector.channelSubset
      if (channelSubset.isEmpty) txChannels else channelSubset
    }

    txTransport.subscribe[Transaction](channels.map(toTopic))(pfCallback)
  }

}

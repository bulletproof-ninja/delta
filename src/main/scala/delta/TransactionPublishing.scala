package delta

import scala.concurrent.Future
import scuff.Subscription
import scuff.Codec

trait TransactionPublishing[SID, EVT]
extends EventStore[SID, EVT] {

  protected def publishTransaction(
      stream: SID, ch: Channel, tx: Future[Transaction]): Unit

  // final abstract override def commit(
  //     channel: Channel, stream: SID, revision: Revision, tick: Tick,
  //     events: List[EVT], metadata: Map[String, String]): Future[Transaction] = {
  //   val tx = super.commit(channel, stream, revision, tick, events, metadata)
  //   publishTransaction(stream, channel, tx)
  //   tx
  // }

}

// /**
//  * Enable pub/sub of transactions.
//  */
// trait MessageTransportPublishing[SID, EVT]
// extends TransactionPublishing[SID, EVT] {

//   protected val txTransport: MessageTransport
//   protected def txTransportCodec: Codec[Transaction, txTransport.InMotion]
//   protected def txChannels: Set[Channel]
//   protected def toTopic(ch: Channel): Topic

//   implicit private lazy val encoder = txTransportCodec.encode _
//   implicit private lazy val decoder = txTransportCodec.decode _

//   protected def publishTransaction(stream: SID, ch: Channel, tx: Future[Transaction]): Unit =
//     txTransport.publish(toTopic(ch), tx)

//   protected type Topic = MessageTransport.Topic
//   protected def Topic(name: String): Topic = MessageTransport.Topic(name)

//   override def subscribe[U](selector: StreamsSelector)(callback: Transaction => U): Subscription = {

//     val channels = {
//       val channelSubset = selector.channelSubset
//       if (channelSubset.isEmpty) txChannels else channelSubset
//     }

//     txTransport.subscribe[Transaction](channels map toTopic) {
//       case tx if selector include tx => callback(tx)
//     }
//   }

// }

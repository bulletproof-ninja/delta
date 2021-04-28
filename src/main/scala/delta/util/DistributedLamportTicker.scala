// package delta.util

// import scala.concurrent.duration._
// import delta._
// import scuff.Codec

// trait DistributedLamportTicker[ID, EVT] {
//   eventStore: EventStore[ID, EVT] =>

//   protected type TransportType
//   protected def transport: MessageTransport[TransportType]

//   protected def txTransportCodec: Codec[Transaction, TransportType]
//   protected def txTransportChannels: Set[Channel]

//   protected lazy val txDist = TransactionDistributor.start(this, transport, txTransportCodec)

//   lazy val ticker: Ticker = {
//     import scuff.concurrent._
//     val initTick = eventStore.maxTick.await(10.seconds) getOrElse -1L
//     LamportTicker.from(txDist, txTransportChannels, initTick)
//   }

// }
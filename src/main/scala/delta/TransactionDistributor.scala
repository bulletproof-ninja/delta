// package delta

// import scuff.Subscription
// import scuff.Codec
// import java.util.concurrent.atomic.AtomicReference

// /**
//   * Distributor of transaction.
//   *
//   * @param es Event source
//   * @param publishChannels Channels to distribute (or empty for all)
//   * @param transport The messaging transport implementation
//   * @param encode Encoding function
//   * @param decode Decoding function
//   */
// private final class TransactionTransport[SID, EVT, TransportType](
//   es: EventSource[SID, EVT],
//   publishChannels: Set[Channel],
//   transport: MessageTransport[TransportType])(
//   implicit
//   encode: Transaction[SID, EVT] => TransportType,
//   decode: TransportType => Transaction[SID, EVT])
// extends TransactionDistributor[SID, EVT] {

//   private val esSub = new AtomicReference[Option[Subscription]](None)

//   private def Topic(tx: Transaction): transport.Topic = Topic(tx.channel)
//   private def Topic(ch: Channel): transport.Topic = transport.Topic(ch.toString)

//   def start(): this.type = {
//     val selector = if (publishChannels.isEmpty) es.Everything else es.ChannelSelector(publishChannels)
//     val sub = es.subscribeLocal(selector) { tx =>
//       transport.publish(Topic(tx), tx)
//     }
//     val started = esSub.compareAndSet(None, Some(sub))
//     if (started) this
//     else {
//       sub.cancel()
//       throw new IllegalStateException("Distribution already started!")
//     }
//   }

//   def subscribeDistributed(subChannels: Set[Channel])(callback: PartialFunction[Transaction, Unit]): Subscription = {
//     val topics = subChannels.map(Topic(_))
//     if (topics.nonEmpty) transport.subscribe(topics)(callback)
//     else new Subscription { def cancel(): Unit = () }
//   }

//   def cancelAll(): Unit = {
//     esSub.getAndSet(None).foreach{ _.cancel() }
//   }

// }

// trait TransactionDistributor[SID, EVT] {
//   type Transaction = delta.Transaction[SID, EVT]

//   /**
//     * Subscribe to transactions from specific channels.
//     * @param channels Subscription channel.
//     * @param more Additional subscription channels.
//     * @param callback The callback function
//     * @return Subscription
//     */
//   final def subscribeDistributed(
//       channel: Channel, more: Channel*)(
//       callback: PartialFunction[Transaction, Unit])
//       : Subscription =
//     subscribeDistributed((more :+ channel).toSet)(callback)

//   /**
//     * Subscribe to transactions.
//     * @param subChannels Subscription channels. Should contain at least one channel
//     * @param callback The callback function
//     * @return Subscription
//     */
//   def subscribeDistributed(
//       subChannels: Set[Channel])(
//       callback: PartialFunction[Transaction, Unit])
//       : Subscription

//   /** Cancel all subscriptions, permanently. */
//   def cancelAllSubscriptions(): Unit

// }

// object TransactionDistributor {

//   /**
//     * Start transaction distribution.
//     * @param es The event source to distribute transactions from
//     * @param transport The messaging transport implementation
//     * @param codec Codec implemenetation
//     * @param channelFilter Filter channels
//     */
//   def start[SID, EVT, TransportType](
//       es: EventSource[SID, EVT],
//       transport: MessageTransport[TransportType],
//       codec: Codec[Transaction[SID, EVT], TransportType],
//       channelFilter: Set[Channel])
//       : TransactionDistributor[SID, EVT] =
//     start(es, transport, channelFilter)(codec.encode, codec.decode)

//   /**
//     * Start transaction distribution.
//     * @param es The event source to distribute transactions from
//     * @param transport The messaging transport implementation
//     * @param codec Codec implemenetation
//     * @param channelFilter Filter channels
//     */
//   def start[SID, EVT, TransportType](
//       es: EventSource[SID, EVT],
//       transport: MessageTransport[TransportType],
//       codec: Codec[Transaction[SID, EVT], TransportType])
//       : TransactionDistributor[SID, EVT] =
//     start(es, transport)(codec.encode, codec.decode)

//   /**
//     * Start transaction distribution.
//     * @param es The event source to distribute transactions from
//     * @param channels Channels to distribute
//     * @param transport The messaging transport implementation
//     * @param encode Encoding function
//     * @param decode Decoding function
//     */
//   def start[SID, EVT, TransportType](
//       es: EventSource[SID, EVT],
//       transport: MessageTransport[TransportType],
//       channelFilter: Set[Channel] = Set.empty)(
//       implicit
//       encode: Transaction[SID, EVT] => TransportType,
//       decode: TransportType => Transaction[SID, EVT])
//       : TransactionDistributor[SID, EVT] = {

//     val dist = new TransactionTransport(es, channelFilter, transport)
//     dist.start()

//   }

// }
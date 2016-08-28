//package ulysses.ddd.cqrs
//
//import ulysses._
//import scala.concurrent.Future
//import scala.concurrent.duration._
//import scuff.Subscription
//import scuff.Timestamp
//import scuff.ddd._
//import language.implicitConversions
//
//trait Generator { projector: Projector =>
//
//  /** Id type. */
//  type ID
//  /** Channel filter type. */
//  type CH
//
//  protected def channelFilter: Set[CH]
//
//  protected def tracker: EventStreamTracker[ID]
//
//  protected type ES = EventStream[ID, _, CH]
//  protected type TXN = ES#TXN
//
//  protected def consume(txn: TXN)(implicit conn: store.RW): Iterable[DAT]
//  protected def publish(msgs: Iterable[DAT])(implicit conn: store.RW)
//
//  /**
//   * Resume event stream processing.
//   * @param eventStream The event stream to consume
//   * @param restart Restart processing from scratch.
//   * NOTICE: This assumes that the data store has also been wiped appropriately
//   */
//  def resume(eventStream: ES): Future[Subscription] = {
//    require(channelFilter.nonEmpty, s"${getClass.getName}: Channel filter cannot be empty")
//    eventStream resume new eventStream.DurableConsumer {
//      val channelFilter = Generator.this.channelFilter
//      def lastTimestamp() = tracker.lastTimestamp
//      def consumeReplay(txn: eventStream.TXN) {
//        val expected = tracker.expectedRevision(txn.stream)
//        if (txn.revision == expected) {
//          store.readWrite(consume(txn)(_))
//          tracker.markAsConsumed(txn.stream, txn.revision, txn.clock)
//        } else if (txn.revision > expected) {
//          throw new IllegalStateException(s"${txn.channel} ${txn.stream} revision(s) missing. Got ${txn.revision}, but was epxecting $expected. This is either revisions missing from the EventSource or a bug in the EventStream implementation.")
//        }
//      }
//      def onLive() = {
//        tracker.onGoingLive()
//        new LiveConsumer {
//          def expectedRevision(streamId: ID): Int = tracker.expectedRevision(streamId)
//          def consumeLive(txn: eventStream.TXN) = store.readWrite { implicit conn =>
//            val forPublish = consume(txn)
//            tracker.markAsConsumed(txn.stream, txn.revision, txn.clock)
//            publish(forPublish)
//          }
//        }
//      }
//    }
//  }
//
//}

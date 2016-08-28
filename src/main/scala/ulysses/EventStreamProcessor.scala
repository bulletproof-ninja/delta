//package ulysses
//
//import java.util.concurrent.{ CountDownLatch, Executors, ScheduledFuture, TimeUnit }
//import scala.concurrent.{ Await, Awaitable, ExecutionContext, Future }
//import scala.concurrent.duration.{ Duration, DurationInt, FiniteDuration }
//import scuff.concurrent._
//import java.util.concurrent.TimeoutException
//import scuff.concurrent.StreamResult
//import scuff.Subscription
//import scala.util.control.NonFatal
//import scala.collection.concurrent.{ Map => CMap }
//import scala.collection.concurrent.TrieMap
//import java.util.concurrent.atomic.AtomicBoolean
//
///**
// * Event stream, which guarantees consistent ordering,
// * even when using distributed protocols that do not.
// * @param es The event source for subscription and replay
// * @param consumerExecCtx The consumer execution context
// * @param replayBuffer Limit the number of in-memory replay transactions.
// * @param gapReplayDelay If revision number gaps are detected, when live, transactions will be replayed after this delay.
// * This should only happen if using unreliable messaging, where messages can get dropped or arrive out-of-order.
// * @param maxClockSkew The max possible clock skew on transaction clocks, in ticks
// * @param maxReplayConsumptionWait The maximum time to wait for replay to finish
// */
//final class EventStreamProcessor[ID, EVT, CH](
//    val es: EventStreamProcessor.ES[ID, EVT, CH],
//    consumerCtx: ExecutionContext) {
//
//  type TXN = Transaction[ID, EVT, CH]
//
//  trait BlockingConsumer {
//    /**
//     * Expected revision for a given stream.
//     * If unknown stream, return 0.
//     */
//    def expectedRevision(stream: ID): Int
//    /** Consume transaction. */
//    def consume(txn: TXN): Unit
//  }
//
//  private abstract class ConsumerContextRunnable(stream: ID) extends Runnable {
//    override final def hashCode = stream.hashCode()
//  }
//
//  private class TransactionConsumerRunnable(txn: TXN, consumer: BlockingConsumer, failedStreams: CMap[ID, (CH, Throwable)])
//      extends ConsumerContextRunnable(txn.stream) {
//
//    private def runWith(txn: TXN) = if (txn == this.txn) this else new TransactionConsumerRunnable(txn, consumer, failedStreams)
//
//    def run {
//      if (!failedStreams.contains(txn.stream)) {
//        val expected = consumer.expectedRevision(txn.stream)
//        if (txn.revision == expected) {
//          try {
//            consumer.consume(txn)
//          } catch {
//            case NonFatal(cause) =>
//              failedStreams.put(txn.stream, txn.channel -> cause)
//              consumerCtx reportFailure new RuntimeException(s"Failed to process ${txn.channel} ${txn.stream}. This stream has been disabled.", cause)
//          }
//        } else if (txn.revision > expected) {
//          val callback = new StreamCallback[TXN] {
//            def onNext(olderTxn: TXN): Unit = consumerCtx execute runWith(olderTxn)
//            def onError(t: Throwable): Unit = t match {
//              case NonFatal(cause) => consumerCtx reportFailure new RuntimeException(s"Failed to replay ${txn.channel} ${txn.stream}", cause)
//              case fatal => throw fatal
//            }
//            def onCompleted(): Unit = consumerCtx execute runWith(txn)
//          }
//          try {
//            es.replayStreamRange(txn.stream, expected until txn.revision)(callback)
//          } catch {
//            case t: Throwable => callback.onError(t)
//          }
//        } else {
//          // Ignore, already processed
//        }
//      }
//    }
//  }
//
//  final class ConsumerSubscription private[EventStreamProcessor] (
//      consumer: BlockingConsumer,
//      failed: CMap[ID, (CH, Throwable)],
//      sub: Subscription) extends Subscription {
//
//    private val cancelled = new AtomicBoolean(false)
//    def cancel() = {
//      if (cancelled.compareAndSet(false, true)) {
//        sub.cancel()
//      }
//    }
//    def checkFailedStreams(): Map[ID, (CH, Throwable)] = failed.toMap
//    def refresh(stream: ID): Unit = if (!cancelled.get) {
//      consumerCtx execute new ConsumerContextRunnable(stream) {
//        def run {
//          failed.remove(stream)
//          val expected = consumer.expectedRevision(stream)
//          val callback = new StreamCallback[TXN] {
//            def onNext(txn: TXN): Unit = consumerCtx execute new TransactionConsumerRunnable(txn, consumer, failed)
//            def onError(t: Throwable): Unit = t match {
//              case NonFatal(cause) => consumerCtx reportFailure new RuntimeException(s"Failed to refresh ${stream}", cause)
//              case fatal => throw fatal
//            }
//            def onCompleted(): Unit = ()
//          }
//          es.replayStreamFrom(stream, expected)(callback)
//        }
//      }
//    }
//  }
//
//  def subscribe(consumer: BlockingConsumer, channels: CH*): ConsumerSubscription = {
//    val failedStreams = new TrieMap[ID, (CH, Throwable)]
//    val channelSet = channels.toSet
//      def channelFilter(ch: CH) = channelSet.isEmpty || channelSet.contains(ch)
//      def proxyConsumer(txn: TXN) {
//        if (!failedStreams.contains(txn.stream)) {
//          consumerCtx execute new TransactionConsumerRunnable(txn, consumer, failedStreams)
//        }
//      }
//    val sub = es.subscribe(proxyConsumer, channelFilter)
//    new ConsumerSubscription(consumer, failedStreams, sub)
//  }
//}
//
//object EventStreamProcessor {
//
//  type ES[ID, EVT, CH] = EventSource[ID, EVT, CH] with Publishing[ID, EVT, CH]
//
//  //  final class StreamsReplayFailure[ID, CH](val failures: Map[ID, (CH, Throwable)]) extends IllegalStateException(s"${failures.size} streams failed processing during replay")
//
//  private val ConsumerThreadFactory = Threads.factory(classOf[EventStreamProcessor[Any, Any, Any]#BlockingConsumer].getName)
//
//  private def schedule(r: Runnable, dur: Duration) = Threads.DefaultScheduler.schedule(r, dur.toMillis, TimeUnit.MILLISECONDS)
//
//  def serializedStreams[ID, EVT, CH](
//    numThreads: Int,
//    es: ES[ID, EVT, CH],
//    failureReporter: Throwable => Unit = (t) => t.printStackTrace(System.err)) = {
//    numThreads match {
//      case 1 =>
//        //        val replayCtx = LockFreeExecutionContext(1, ConsumerThreadFactory, failureReporter = failureReporter)
//        val liveCtx = Threads.newSingleThreadExecutor(ConsumerThreadFactory, failureReporter)
//        new EventStreamProcessor(es, liveCtx)
//      case n =>
//        //        val replayThreads = for (_ <- 1 to numThreads) yield LockFreeExecutionContext(1, ConsumerThreadFactory, failureReporter)
//        //          def shutdownThreads: Future[Unit] = {
//        //            import ExecutionContext.Implicits.global
//        //            Future.sequence(replayThreads.map(_.shutdown)).map(_ => ())
//        //          }
//        //        val replayCtx = new HashPartitionExecutionContext(replayThreads, shutdownThreads, failureReporter)
//        val liveCtx = HashPartitionExecutionContext(n, ConsumerThreadFactory, failureReporter = failureReporter)
//        new EventStreamProcessor(es, liveCtx)
//    }
//  }
//}

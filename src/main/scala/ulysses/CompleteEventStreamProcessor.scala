package ulysses

import java.util.concurrent.{ CountDownLatch, Executors, ScheduledFuture, TimeUnit }
import scala.concurrent.{ Await, Awaitable, ExecutionContext, Future }
import scala.concurrent.duration.{ Duration, DurationInt, FiniteDuration }
import scuff.concurrent._
import java.util.concurrent.TimeoutException
import scuff.concurrent.StreamResult
import scuff.Subscription

/**
  * Event stream, which guarantees consistent ordering,
  * even when using distributed protocols that do not.
  * @param es The event source for subscription and replay
  * @param consumerExecCtx The consumer execution context
  * @param replayBuffer Limit the number of in-memory replay transactions.
  * @param gapReplayDelay If revision number gaps are detected, when live, transactions will be replayed after this delay.
  * This should only happen if using unreliable messaging, where messages can get dropped or arrive out-of-order.
  * @param maxClockSkew The max possible clock skew on transaction clocks, in ticks
  * @param maxReplayConsumptionWait The maximum time to wait for replay to finish
  */
final class CompleteEventStreamProcessor[ID, EVT, CAT](
    val es: CompleteEventStreamProcessor.ES[ID, EVT, CAT],
    replayCtx: ExecutionContext,
    replayShutdown: => Future[Unit],
    liveCtx: ExecutionContext,
    //    replayBuffer: Int,
    gapReplayDelay: FiniteDuration,
    maxClockSkew: Int) {

  def this(es: CompleteEventStreamProcessor.ES[ID, EVT, CAT],
           consumerCtx: ExecutionContext,
           //    replayBuffer: Int,
           gapReplayDelay: FiniteDuration,
           maxClockSkew: Int) = this(es, consumerCtx, Future.successful(()), consumerCtx, gapReplayDelay, maxClockSkew)

  type TXN = es.Transaction

  /**
    * A durable consumer goes through two
    * stages,
    * 1) Replay mode. First time run or
    * resumption after downtime will result
    * in feeding of replayed data.
    * 2) Live mode. Once replay is done,
    * consumption becomes live.
    */
  trait DurableConsumer {
    trait LiveConsumer {
      /**
        * Expected revision for a given stream.
        * If unknown stream, return 0.
        */
      def expectedRevision(stream: ID): Int
      /** Consume live transaction. */
      def consumeLive(txn: TXN)
    }
    /** Last transaction clock. */
    def lastTimestamp(): Option[Long]

    /**
      * Called when live consumption starts.
      */
    def onLive(): LiveConsumer

    /** Consume replay transaction. */
    def consumeReplay(txn: TXN)

    /** Categories. Empty means all. */
    def categoryFilter: Set[CAT]
  }

  private val _failedStreams = new LockFreeConcurrentMap[ID, (CAT, Throwable)]

  def failedStreams = _failedStreams.snapshot()

  //  private class BlockingReplayProxy(consumer: DurableConsumer) {
  //    private[this] val awaitQueue = new java.util.concurrent.ArrayBlockingQueue[(TXN, Awaitable[_])](replayBuffer)
  //    //    private[this] var lastTime = -1L
  //    @volatile var doneReading = false
  //    private[this] val awaiterLatch = new CountDownLatch(1)
  //    private val awaiter = new Runnable {
  //      def run {
  //        while (!Thread.currentThread.isInterrupted) {
  //          awaitQueue.poll(1, TimeUnit.SECONDS) match {
  //            case null =>
  //              if (doneReading) {
  //                awaiterLatch.countDown()
  //                return
  //              }
  //            case (txn, awaitable) =>
  //              try {
  //                Await.result(awaitable, 60.seconds)
  //              } catch {
  //                case toe: TimeoutException =>
  //                  throw new IllegalStateException(s"${consumer.getClass.getName} timed out processing $txn", toe)
  //              }
  //          }
  //        }
  //      }
  //    }
  //    def processBlocking(replay: Iterator[Transaction]): Unit = {
  //        def consume(txn: Transaction) = try {
  //          consumer consumeReplay txn
  //        } catch {
  //          case e: Exception =>
  //            throw new IllegalStateException(s"${consumer.getClass.getName} failed to process $txn", e)
  //        }
  //      Threads.Blocking.execute(awaiter)
  //      consumerCtx match {
  //        case ctx: HashBasedSerialExecutionContext =>
  //          replay.foreach { txn =>
  //            awaitQueue put txn -> ctx.submit(txn.streamId.hashCode)(consume(txn))
  //          }
  //        case ctx =>
  //          replay.foreach { txn =>
  //            awaitQueue put txn -> Future(consume(txn))(ctx)
  //          }
  //      }
  //      doneReading = true
  //      if (maxReplayConsumptionWait.isFinite) {
  //        if (!awaiterLatch.await(maxReplayConsumptionWait.length, maxReplayConsumptionWait.unit)) {
  //          throw new TimeoutException(s"Replay processing exceeded $maxReplayConsumptionWait")
  //        }
  //      } else {
  //        awaiterLatch.await()
  //      }
  //      //      if (lastTime == -1L) None else Some(lastTime)
  //    }
  //
  //  }
  //
  //  private class LiveConsumerProxy(consumer: DurableConsumer#LiveConsumer) extends (Transaction => Unit) {
  //    def apply(txn: Transaction) = consumer.consumeLive(txn)
  //  }

  private[this] val pendingReplays = new LockFreeConcurrentMap[ID, ScheduledFuture[_]]

  //  private def LiveConsumerProxy(consumer: DurableConsumer#LiveConsumer) =
  //    new LiveConsumerProxy(consumer) with util.FailSafeTransactionHandler[ID, EVT, CAT] with util.SequencedTransactionHandler[ID, EVT, CAT] with util.AsyncTransactionHandler[ID, EVT, CAT] { self: LiveConsumerProxy =>
  //      def asyncTransactionCtx = consumerCtx
  //      def onGapDetected(id: ID, expectedRev: Int, actualRev: Int) {
  //        if (!pendingReplays.contains(id)) {
  //          val replayer = new Runnable {
  //            def run = es.replayStreamRange(id, expectedRev until actualRev)(_.foreach(self))
  //          }
  //          val futureReplayer = EventStream.schedule(replayer, gapReplayDelay)
  //          if (pendingReplays.putIfAbsent(id, futureReplayer).isDefined) futureReplayer.cancel(false)
  //        }
  //      }
  //      def onGapClosed(id: ID) {
  //        pendingReplays.get(id) match {
  //          case Some(futureReplayer) =>
  //            futureReplayer.cancel(false)
  //            pendingReplays.remove(id, futureReplayer)
  //          case _ => // Ignore
  //        }
  //      }
  //      def expectedRevision(streamId: ID): Int = consumer.expectedRevision(streamId)
  //      def isFailed(streamId: ID) = _failedStreams.contains(streamId)
  //      def markFailed(streamId: ID, cat: CAT, t: Throwable) {
  //        _failedStreams.update(streamId, cat -> t)
  //        consumerCtx.reportFailure(t)
  //      }
  //    }

  /**
    *  Resume consumption.
    *  @return Live subscription. The future is resolved once consumption goes live.
    */
  //  def resume(consumer: DurableConsumer): Future[Subscription] = {
  //      implicit def ec = Threads.PiggyBack
  //    val categorySet = consumer.categoryFilter
  //      def categoryFilter(cat: CAT) = categorySet.isEmpty || categorySet.contains(cat)
  //      def replayConsumer(txns: Iterator[Transaction]): Unit = {
  //        val replayConsumer = new BlockingReplayProxy(consumer)
  //        replayConsumer.processBlocking(txns)
  //      }
  //    val replayFinished: Future[Long] = es.lastTimestamp.flatMap { startupTimestamp =>
  //      val replayFinished = consumer.lastTimestamp match {
  //        case None => es.replay(categorySet.toSeq: _*)(replayConsumer)
  //        case Some(lastProcessed) =>
  //          val replaySince = lastProcessed - maxClockSkew
  //          es.replayFrom(replaySince, categorySet.toSeq: _*)(replayConsumer)
  //      }
  //      replayFinished.map(_ => startupTimestamp)
  //    }
  //    val futureSub = replayFinished.flatMap { startupTimestamp =>
  //      if (_failedStreams.nonEmpty) {
  //        throw new EventStream.StreamsReplayFailure(_failedStreams.snapshot)
  //      }
  //      val liveConsumer = LiveConsumerProxy(consumer.onLive())
  //      val sub = es.subscribe(liveConsumer, categoryFilter)
  //      val replaySince = startupTimestamp - maxClockSkew
  //      // Close the race condition; replay anything missed between replay and subscription
  //      es.replayFrom(replaySince, categorySet.toSeq: _*)(_.foreach(liveConsumer)).map(_ => sub)
  //    }
  //    futureSub.onFailure {
  //      case t => consumerCtx.reportFailure(t)
  //    }
  //    futureSub
  //  }

  private final class TxnRunnable(txn: TXN, f: TXN => Unit) extends Runnable {
    def run = f(txn)
    override def hashCode = txn.streamId.hashCode()
  }

  private def consumeReplay(consumer: TXN => Unit)(txn: TXN) = replayCtx execute new TxnRunnable(txn, consumer)
  private def consumeLive(consumer: TXN => Unit)(txn: TXN) = liveCtx execute new TxnRunnable(txn, consumer)

  def resume(consumer: DurableConsumer): Future[Subscription] = {
    import ExecutionContext.Implicits.global

    val categorySet = consumer.categoryFilter
      def categoryFilter(cat: CAT) = categorySet.isEmpty || categorySet.contains(cat)
    val categories = categorySet.toSeq
    val replay = consumer.lastTimestamp() match {
      case Some(lastConsumerTimestamp) =>
        Future.successful(es.replayFrom(lastConsumerTimestamp - maxClockSkew, categories: _*) _)
      case None =>
        es.minClock()
        es.replay(categories: _*) _
    }
    val replayConsumer = consumeReplay(consumer.consumeReplay) _
    val replayFinished = StreamResult.fold(replay)(Long.MinValue) {
      case (_, txn) =>
        replayConsumer(txn)
        txn.clock
    }
    val replayConsumed = replayFinished.flatMap(lastTimestamp => replayShutdown.map(_ => lastTimestamp))
    replayConsumed.flatMap { lastKnownTimestamp =>
      if (_failedStreams.nonEmpty) {
        throw new CompleteEventStreamProcessor.StreamsReplayFailure(_failedStreams.snapshot)
      }
      val live = consumer.onLive()
      val liveConsumer = consumeLive(live.consumeLive) _
      val subscription = es.subscribe(liveConsumer, categoryFilter)
      val replaySince = lastKnownTimestamp - maxClockSkew
      // Close the race condition; replay anything possibly missed between replay and subscription
      StreamResult.fold(es.replayFrom(replaySince, categories: _*))(subscription) {
        case (sub, txn) =>
          liveConsumer(txn)
          sub
      }
    }
  }
}

object CompleteEventStreamProcessor {

  type ES[ID, EVT, CAT] = EventSource[ID, EVT, CAT] with Publishing[ID, EVT, CAT] with TimeOrdering[ID, EVT, CAT]

  final class StreamsReplayFailure[ID, CAT](val failures: Map[ID, (CAT, Throwable)]) extends IllegalStateException(s"${failures.size} streams failed processing during replay")

  private val ConsumerThreadFactory = Threads.factory(classOf[CompleteEventStreamProcessor[Any, Any, Any]#DurableConsumer].getName)

  private def schedule(r: Runnable, dur: Duration) = Threads.DefaultScheduler.schedule(r, dur.toMillis, TimeUnit.MILLISECONDS)

  def serializedStreams[ID, EVT, CAT](
    numThreads: Int,
    es: ES[ID, EVT, CAT],
    replayBufferSize: Int,
    gapReplayDelay: FiniteDuration,
    maxClockSkew: Int,
    //    maxReplayConsumptionWait: Duration,
    failureReporter: Throwable => Unit = (t) => t.printStackTrace(System.err)) = {
    numThreads match {
      case 1 =>
        val replayCtx = LockFreeExecutionContext(1, ConsumerThreadFactory, failureReporter = failureReporter)
        val liveCtx = Threads.newSingleThreadExecutor(ConsumerThreadFactory, failureReporter)
        new CompleteEventStreamProcessor(es, replayCtx, replayCtx.shutdown(), liveCtx, gapReplayDelay, maxClockSkew)
      case n =>
        val replayThreads = for (_ <- 1 to numThreads) yield LockFreeExecutionContext(1, ConsumerThreadFactory, failureReporter)
          def shutdownThreads: Future[Unit] = {
            import ExecutionContext.Implicits.global
            Future.sequence(replayThreads.map(_.shutdown)).map(_ => ())
          }
        val replayCtx = new HashPartitionExecutionContext(replayThreads, shutdownThreads, failureReporter)
        val liveCtx = HashPartitionExecutionContext(n, ConsumerThreadFactory, failureReporter)
        new CompleteEventStreamProcessor(es, replayCtx, replayCtx.shutdown(), liveCtx, gapReplayDelay, maxClockSkew)
    }
  }
}

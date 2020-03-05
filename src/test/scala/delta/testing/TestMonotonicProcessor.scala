package delta.testing

import java.util.concurrent.{ CountDownLatch, TimeUnit }

import scala.collection.concurrent.TrieMap

import org.junit.Assert._
import org.junit.Test

import delta.{ Projector, Snapshot, Transaction }
import Transaction.Channel
import scuff.concurrent.{ PartitionedExecutionContext, ScuffFutureObject }
import scala.concurrent._, duration._
import scala.util.{ Random => rand }
import delta.process.MonotonicReplayProcessor
import delta.process.ConcurrentMapStore
import delta.process.Update

class TestMonotonicProcessor {
  private[this] val NoFallback = (_: Int) => Future.none

  @Test
  def `test out-of-order and callback`(): Unit = {
    object Tracker {
      def clear() = {
        snapshotMap.clear()
        latch = new CountDownLatch(1)
      }
      type State = ConcurrentMapStore.State[String]
      val snapshotMap = new TrieMap[Int, State]
      @volatile var lastUpdate: Update[String] = _
      @volatile var latch: CountDownLatch = _
    }
    class Mono(ec: ExecutionContext)
      extends MonotonicReplayProcessor[Int, Char, String, String, Unit](
        20.seconds,
        ConcurrentMapStore(Tracker.snapshotMap, None)(NoFallback)) {
      def whenDone() = Future successful (())
      override def onUpdate(id: Int, update: Update) = {
        //        println(s"Update: ${update.snapshot}, Latch: ${Tracker.latch.getCount}")
//        assertEquals(42, update.id)
        assertTrue(update.changed.nonEmpty)
        Tracker.lastUpdate = update
        if (update.revision == 4) {
          assertEquals(1L, Tracker.latch.getCount)
          Tracker.latch.countDown()
        }
      }
      object Concat extends Projector[String, Char] {
        def init(c: Char) = new String(Array(c))
        def next(str: String, c: Char) = s"$str$c"
      }
      def processContext(id: Int) = ec
      def process(tx: Transaction, state: Option[String]) = {
        val newState = Concat(tx, state)
        //        println(s"Tx ${tx.revision}: $state => $newState")
        newState
      }
    }
    val Ch = Channel("")
    val txs = List(
      Transaction(
        stream = 42, revision = 0,
        events = List('H', 'e', 'l', 'l'),
        tick = 100, channel = Ch, metadata = Map.empty),
      Transaction(
        stream = 42, revision = 1,
        events = List('o', ','),
        tick = 105, channel = Ch, metadata = Map.empty),
      Transaction(
        stream = 42, revision = 2,
        events = List(' '),
        tick = 220, channel = Ch, metadata = Map.empty),
      Transaction(
        stream = 42, revision = 0,
        events = List('H', 'e', 'l', 'l'),
        tick = 100, channel = Ch, metadata = Map.empty),
      Transaction(
        stream = 42, revision = 3,
        events = List('W', 'o'),
        tick = 300, channel = Ch, metadata = Map.empty),
      Transaction(
        stream = 42, revision = 4,
        events = List('r', 'l', 'd', '!'),
        tick = 666, channel = Ch, metadata = Map.empty))

      def processAndVerify(ec: ExecutionContext, txs: List[Transaction[Int, Char]], n: Int): Unit = {
        Tracker.clear()
        val txSequence = { txs.map(_.revision).mkString(",") }
        //        println(s"----------------- $n: $txSequence using $ec ---------------")
        val mp = new Mono(ec)
        txs.map(mp).foreach(_.await)
        assertTrue(
          s"(Latch: ${Tracker.latch.getCount}) Timed out on number $n with the following revision sequence: $txSequence, using EC $ec",
          Tracker.latch.await(5, TimeUnit.SECONDS))
        assertEquals(Snapshot("Hello, World!", 4, 666), Tracker.snapshotMap(42).snapshot)
        assertEquals(Update(Some("Hello, World!"), 4, 666), Tracker.lastUpdate)
      }

    val partitioned = PartitionedExecutionContext(1, th => th.printStackTrace(System.err))
    val ecs = List(partitioned.singleThread(42), ExecutionContext.global)
    ecs.foreach(processAndVerify(_, txs, 1))
    ecs.foreach(processAndVerify(_, txs.reverse, 2))
    for (n <- 3 to 1000) {
      val random = rand.shuffle(txs)
      ecs.foreach(processAndVerify(_, random, n))
    }
    for (n <- 1001 to 1025) {
      processAndVerify(RandomDelayExecutionContext, rand.shuffle(txs), n)
    }

  }

  @Test
  def `large entity`(): Unit = {
      def test(exeCtx: ExecutionContext): Unit = {
          implicit def ec = exeCtx
        //        println(s"Testing with $exeCtx")
        type State = ConcurrentMapStore.State[String]
        val snapshotMap = new TrieMap[Int, State]
        val processor = new MonotonicReplayProcessor[Int, Char, String, String, Unit](
          20.seconds,
          ConcurrentMapStore(snapshotMap, None)(NoFallback)) {
          protected def processContext(id: Int): ExecutionContext = ec match {
            case ec: PartitionedExecutionContext => ec.singleThread(id)
            case ec => ec
          }
          protected def whenDone() = Future successful (())
          protected def process(tx: Transaction, currState: Option[String]) = {
            val sb = new StringBuilder(currState getOrElse "")
            tx.events.foreach {
              case char: Char => sb += char
            }
            sb.result()
          }
        }
        val id = rand.nextInt
        val txCount = 5000
        val txs = (0 until txCount).map { rev =>
          val events = (0 to rand.nextInt(5)).map(_ => rand.nextPrintableChar).toList
          new Transaction(
            tick = rev,
            channel = Channel("Chars"),
            stream = id,
            revision = rev,
            metadata = Map.empty[String, String],
            events = events)
        }
        val expectedString: String = txs.flatMap(_.events).mkString
        val randomized = rand.shuffle(txs)
        val processFutures = randomized.map { tx =>
          processor(tx)
        }
        val allProcessed = Future.sequence(processFutures)
        allProcessed.await
        val ConcurrentMapStore.State(Snapshot(string, revision, _), modified) = snapshotMap(id)
        assertTrue(modified)
        assertEquals(expectedString, string)
        assertEquals(txCount - 1, revision)
      }

    test(ExecutionContext.global)
    test(PartitionedExecutionContext(1, th => th.printStackTrace(System.err)))
    test(new RandomDelayExecutionContext(ExecutionContext.global, 3))

  }

}

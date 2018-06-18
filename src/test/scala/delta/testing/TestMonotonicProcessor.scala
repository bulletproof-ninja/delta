package delta.testing

import java.util.concurrent.{ CountDownLatch, TimeUnit }

import scala.collection.concurrent.TrieMap

import org.junit.Assert._
import org.junit.Test

import delta.{ EventReducer, Snapshot, Transaction }
import delta.util._
import scuff.concurrent.PartitionedExecutionContext
import scala.concurrent._, duration._
import scala.util.{ Random => rand }

class TestMonotonicProcessor {
  private val NoFuture = Future successful None
  private val NoFallback = (r: Int) => NoFuture

  @Test
  def `test out-of-order and callback`(): Unit = {
    object Tracker {
      def clear() = {
        snapshotMap.clear()
        latch = new CountDownLatch(1)
      }
      val snapshotMap = new TrieMap[Int, Snapshot[String]]
      @volatile var lastSnapshotUpdate: Snapshot[String] = _
      @volatile var latch: CountDownLatch = _
    }
    class Mono(ec: ExecutionContext)
      extends MonotonicBatchProcessor[Int, Char, String, Unit](
        20.seconds,
        new ConcurrentMapStore(Tracker.snapshotMap)(NoFallback)) {
      def whenDone() = Future successful (())
      override def onUpdate(key: Int, update: Update) = {
        //        println(s"Update: ${update.snapshot}, Latch: ${Tracker.latch.getCount}")
        assertEquals(42, key)
        Tracker.lastSnapshotUpdate = update.snapshot
        if (update.snapshot.revision == 4) {
          assertEquals(1L, Tracker.latch.getCount)
          Tracker.latch.countDown()
        }
      }
      object Concat extends EventReducer[String, Char] {
        def init(c: Char) = new String(Array(c))
        def next(str: String, c: Char) = s"$str$c"
        val process = EventReducer.process(this) _
      }
      def processingContext(id: Int) = ec
      def process(txn: TXN, state: Option[String]) = {
        val newState = Concat.process(state, txn.events)
        //        println(s"Txn ${txn.revision}: $state => $newState")
        newState
      }
    }
    val txns = List(
      Transaction(
        stream = 42, revision = 0,
        events = List('H', 'e', 'l', 'l'),
        tick = 100, channel = "", metadata = Map.empty),
      Transaction(
        stream = 42, revision = 1,
        events = List('o', ','),
        tick = 105, channel = "", metadata = Map.empty),
      Transaction(
        stream = 42, revision = 2,
        events = List(' '),
        tick = 220, channel = "", metadata = Map.empty),
      Transaction(
        stream = 42, revision = 0,
        events = List('H', 'e', 'l', 'l'),
        tick = 100, channel = "", metadata = Map.empty),
      Transaction(
        stream = 42, revision = 3,
        events = List('W', 'o'),
        tick = 300, channel = "", metadata = Map.empty),
      Transaction(
        stream = 42, revision = 4,
        events = List('r', 'l', 'd', '!'),
        tick = 666, channel = "", metadata = Map.empty))

      def processAndVerify(ec: ExecutionContext, txns: List[Transaction[Int, Char]], n: Int): Unit = {
        Tracker.clear()
        val txnSequence = { txns.map(_.revision).mkString(",") }
        //        println(s"----------------- $n: $txnSequence using $ec ---------------")
        val mp = new Mono(ec)
        txns.map(mp).foreach(_.await)
        assertTrue(
          s"(Latch: ${Tracker.latch.getCount}) Timed out on number $n with the following revision sequence: $txnSequence, using EC $ec",
          Tracker.latch.await(5, TimeUnit.SECONDS))
        assertEquals(Snapshot("Hello, World!", 4, 666), Tracker.snapshotMap(42))
        assertEquals(Snapshot("Hello, World!", 4, 666), Tracker.lastSnapshotUpdate)
      }

    val partitioned = PartitionedExecutionContext(1, th => th.printStackTrace(System.err))
    val ecs = List(partitioned.singleThread(42), ExecutionContext.global)
    ecs.foreach(processAndVerify(_, txns, 1))
    ecs.foreach(processAndVerify(_, txns.reverse, 2))
    for (n <- 3 to 1000) {
      val random = rand.shuffle(txns)
      ecs.foreach(processAndVerify(_, random, n))
    }
    for (n <- 1001 to 1025) {
      processAndVerify(RandomDelayExecutionContext, rand.shuffle(txns), n)
    }

  }

  @Test
  def `large entity`(): Unit = {
      def test(exeCtx: ExecutionContext): Unit = {
          implicit def ec = exeCtx
        //        println(s"Testing with $exeCtx")
        val snapshotMap = new TrieMap[Int, Snapshot[String]]
        val processor = new MonotonicBatchProcessor[Int, Char, String, Unit](
          20.seconds,
          new ConcurrentMapStore(snapshotMap)(NoFallback)) {
          protected def processingContext(id: Int): ExecutionContext = ec match {
            case ec: PartitionedExecutionContext => ec.singleThread(id)
            case ec => ec
          }
          protected def whenDone() = Future successful (())
          protected def process(txn: TXN, currState: Option[String]): String = {
            val sb = new StringBuilder(currState getOrElse "")
            txn.events.foreach {
              case char: Char => sb += char
            }
            sb.result()
          }
        }
        val id = rand.nextInt
        val txnCount = 5000
        val txns = (0 until txnCount).map { rev =>
          val events = (0 to rand.nextInt(5)).map(_ => rand.nextPrintableChar).toList
          new Transaction(
            tick = rev,
            channel = "Chars",
            stream = id,
            revision = rev,
            metadata = Map.empty[String, String],
            events = events)
        }
        val expectedString: String = txns.flatMap(_.events).mkString
        val randomized = rand.shuffle(txns)
        val processFutures = randomized.map { txn =>
          processor(txn)
        }
        val allProcessed = Future.sequence(processFutures)
        allProcessed.await
        val Snapshot(string, revision, _) = snapshotMap(id)
        assertEquals(expectedString, string)
        assertEquals(txnCount - 1, revision)
      }

    test(ExecutionContext.global)
    test(PartitionedExecutionContext(1, th => th.printStackTrace(System.err)))
    test(new RandomDelayExecutionContext(ExecutionContext.global, 3))

  }

}

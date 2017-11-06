package delta.testing

import java.util.concurrent.{ CountDownLatch, TimeUnit }

import scala.collection.concurrent.TrieMap

import org.junit.Assert._
import org.junit.Test

import delta.{ Fold, Snapshot, Transaction }
import delta.util.{ ConcurrentMapStore, MonotonicProcessor }
import scuff.concurrent.PartitionedExecutionContext
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

class TestMonotonicProcessor {
  @Test
  def `test out-of-order and callback`() {
    object Tracker {
      def clear() = {
        snapshotMap.clear()
        latch = new CountDownLatch(1)
      }
      val snapshotMap = new TrieMap[Int, Option[Snapshot[String]]]
      @volatile var lastSnapshotUpdate: Snapshot[String] = _
      @volatile var latch: CountDownLatch = _
    }
    class Mono(ec: ExecutionContext) extends MonotonicProcessor[Int, Char, String] {
      def onMissingRevisions(id: Int, missing: Range) = ()
      def onUpdate(key: Int, update: Update) = {
        //        println(s"Update: ${update.snapshot}, Latch: ${Tracker.latch.getCount}")
        assertEquals(42, key)
        Tracker.lastSnapshotUpdate = update.snapshot
        if (update.snapshot.revision == 4) {
          assertEquals(1L, Tracker.latch.getCount)
          Tracker.latch.countDown()
        }
      }
      object Concat extends Fold[String, Char] {
        def init(c: Char) = new String(Array(c))
        def next(str: String, c: Char) = s"$str$c"
      }
      val processStore = new ConcurrentMapStore(Tracker.snapshotMap, (_: Int) => Future successful None)
      def executionContext(id: Int) = ec
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
        tick = 100, channel = (), metadata = Map.empty),
      Transaction(
        stream = 42, revision = 1,
        events = List('o', ','),
        tick = 105, channel = (), metadata = Map.empty),
      Transaction(
        stream = 42, revision = 2,
        events = List(' '),
        tick = 220, channel = (), metadata = Map.empty),
      Transaction(
        stream = 42, revision = 0,
        events = List('H', 'e', 'l', 'l'),
        tick = 100, channel = (), metadata = Map.empty),
      Transaction(
        stream = 42, revision = 3,
        events = List('W', 'o'),
        tick = 300, channel = (), metadata = Map.empty),
      Transaction(
        stream = 42, revision = 4,
        events = List('r', 'l', 'd', '!'),
        tick = 666, channel = (), metadata = Map.empty))

      def processAndVerify(ec: ExecutionContext, txns: List[Transaction[Int, Char, Unit]], n: Int) {
        Tracker.clear()
        val txnSequence = { txns.map(_.revision).mkString(",") }
        //        println(s"----------------- $n: $txnSequence using $ec ---------------")
        val mp = new Mono(ec)
        txns.map(mp).foreach(_.await)
        assertTrue(
          s"(Latch: ${Tracker.latch.getCount}) Timed out on number $n with the following revision sequence: $txnSequence, using EC $ec",
          Tracker.latch.await(5, TimeUnit.SECONDS))
        assertEquals(Snapshot("Hello, World!", 4, 666), Tracker.snapshotMap(42).get)
        assertEquals(Snapshot("Hello, World!", 4, 666), Tracker.lastSnapshotUpdate)
      }

    val partitioned = PartitionedExecutionContext(1)
    val ecs = List(partitioned.singleThread(42), ExecutionContext.global)
    ecs.foreach(processAndVerify(_, txns, 1))
    ecs.foreach(processAndVerify(_, txns.reverse, 2))
    for (n <- 3 to 1000) {
      val random = util.Random.shuffle(txns)
      ecs.foreach(processAndVerify(_, random, n))
    }
    for (n <- 1001 to 1025) {
      processAndVerify(RandomDelayExecutionContext, util.Random.shuffle(txns), n)
    }

  }

}

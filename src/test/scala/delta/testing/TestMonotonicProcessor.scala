package delta.testing

import java.util.concurrent.{ CountDownLatch, TimeUnit }

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Buffer

import org.junit.Assert._
import org.junit.Test

import delta.{ Fold, Snapshot, Transaction }
import delta.util.{ ConcurrentMapSnapshotStore, MonotonicProcessor }
import scuff.concurrent.PartitionedExecutionContext

class TestMonotonicProcessor {
  @Test
  def `test out-of-order and callback`() {
    object Tracker {
      def clear() = {
        snapshotMap.clear()
        latch = new CountDownLatch(1)
      }
      val snapshotMap = new TrieMap[Int, Snapshot[String]]
      var lastSnapshotUpdate: Snapshot[String] = _
      @volatile var latch: CountDownLatch = _
    }
    val mp = new MonotonicProcessor[Int, Char, Unit, String] {
      def onSnapshotUpdated(key: Int, newSnapshot: Snapshot[String]) {
        assertEquals(42, key)
        Tracker.lastSnapshotUpdate = newSnapshot
        if (newSnapshot.revision == 4) Tracker.latch.countDown()
      }
      object Concat extends Fold[String, Char] {
        def init(c: Char) = new String(Array(c))
        def next(str: String, c: Char) = s"$str$c"
      }
      val snapshots = new ConcurrentMapSnapshotStore(Tracker.snapshotMap)
      val exeCtx = PartitionedExecutionContext(1)
      def process(txn: TXN, state: Option[String]) = Concat.process(state, txn.events)
    }
    val txns = java.util.Arrays.asList(
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

      def processAndVerify(txns: Buffer[Transaction[Int, Char, Unit]], n: Int) {
        Tracker.clear()
        txns.foreach(mp.apply)
        assertTrue(
          s"Timed out on number $n with the following revision sequence: ${txns.map(_.revision).mkString(",")}",
          Tracker.latch.await(2, TimeUnit.SECONDS))
        assertEquals(Snapshot("Hello, World!", 4, 666), Tracker.snapshotMap(42))
        assertEquals(Snapshot("Hello, World!", 4, 666), Tracker.lastSnapshotUpdate)
      }

    processAndVerify(txns.asScala, 1)
    processAndVerify(txns.asScala.reverse, 2)
    for (n <- 3 to 100) {
      java.util.Collections.shuffle(txns)
      processAndVerify(txns.asScala, n)
    }

  }

}

package ulysses.hazelcast

import scuff.LamportClock.CASLong
import com.hazelcast.core.IAtomicLong
import ulysses.EventSource
import scuff.concurrent._
import concurrent.duration._

object LamportClock {
  def apply(al: IAtomicLong, es: EventSource[_, _, _]): ulysses.LamportClock = {
    val cas = new AtomicCAS(al)
    val lc = new scuff.LamportClock(cas)
    es.lastTick().await(111.seconds).foreach(lc.sync)
    ulysses.LamportClock(lc)
  }
}

private class AtomicCAS(al: IAtomicLong) extends CASLong {
  def value: Long = al.get
  def compAndSwap(expected: Long, update: Long): Boolean = al.compareAndSet(expected, update)
  def incrAndGet(): Long = al.incrementAndGet()
}

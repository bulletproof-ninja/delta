package delta.hazelcast

import scala.concurrent.duration.DurationInt

import com.hazelcast.core.IAtomicLong

import scuff.LamportClock
import scuff.LamportClock.CASLong
import scuff.concurrent.ScuffScalaFuture
import delta.EventSource
import delta.LamportTicker

object AtomicLongLamportTicker {
  def apply(al: IAtomicLong, es: EventSource[_, _, _]): LamportTicker = {
    val cas = new AtomicCAS(al)
    val lc = new LamportClock(cas)
    es.lastTick().await(111.seconds).foreach(lc.sync)
    LamportTicker(lc)
  }
}

private class AtomicCAS(al: IAtomicLong) extends CASLong {
  def value: Long = al.get
  def compAndSwap(expected: Long, update: Long): Boolean = al.compareAndSet(expected, update)
  def incrAndGet(): Long = al.incrementAndGet()
}

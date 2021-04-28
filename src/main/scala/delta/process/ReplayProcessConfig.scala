package delta.process

import delta.Tick
import scala.concurrent.duration.FiniteDuration

/**
  * Replay process confguration.
  *
  * @param completionTimeout
  * The time to wait for completion processing of remaining transactions,
  * ''after'' all input has been exhausted.
  *
  * A stream is considered incomplete if there are missing revisions (typically
  * `tickWindow` too small) or if there are errors in processing.
  *
  * @param writeBatchSize
  * The batch size used to persist replay state.
  * A value of 1 is effectively no batching.
  * Must be strictly > 0.
  *
  * @param writeTickOrdered
  * Write state in tick order. This has ultimately no effect ''if''
  * all writes succeeds. However, if the persistence phase is
  * interrupted, the tick watermark becomes unreliable, which
  * potentially can lead to missed transaction when replay is resumed.
  * The cost is that memory use increases temporarily and the persistence
  * phase is potentially slower.
  * Defaults to `true`
  *
  * @param tickWindow
  * The largest reasonable tick skew combined with network
  * propagation delay, and in other factors that might affect
  * tick differences.
  *
  * Note: A tick window that's _too large_ can lead to unnecessary
  * processing of already processed transactions. But with idempotent
  * processing, this will not affect correctness.
  * Likewise, a tick window that's too small could possibly
  * miss a window of unprocessed transactions, which will then be replayed
  * individually. Again, this will not affect correctness, but is ineffecient.
  *
  * Cannot be negative.
  *
  * Defaults to `MaxValue`, i.e. unbounded, which means that the entire event
  * source history is consumed. This is likely to get ineffecient over time, but
  * is a very safe default.
  *
  * @param maxBlockingQueueSize
  * If processing is slow, it can be overwhelmed by data, leading to OOM errors.
  * By settings a maximum queue size, the producer will be blocked instead of
  * running out of memory.
  */
final case class ReplayProcessConfig(
  completionTimeout: FiniteDuration,
  writeBatchSize: Int,
  writeTickOrdered: Boolean = true,
  tickWindow: Int = Int.MaxValue,
  maxBlockingQueueSize: Int = Int.MaxValue) {

  require(writeBatchSize >= 1, s"Must have sensible batch size, not: $writeBatchSize")
  require(tickWindow >= 0, s"Cannot have negative tick window: $tickWindow")

  /** @return Tick to replay from or `None` to replay since beginning of time. */
  def adjustToWindow(watermark: Option[Tick]): Option[Tick] = {
    watermark.flatMap { tickWatermark =>
      if (tickWindow == Int.MaxValue) None
      else ((tickWatermark - tickWindow) min tickWatermark) match {
        case `tickWatermark` => None
        case adjusted => Some(adjusted)
      }
    }
  }

}

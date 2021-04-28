package delta.process

import scuff.concurrent.PartitionedExecutionContext
import scala.concurrent._
import scuff.concurrent.Threads
import scala.collection.concurrent.{ Map => CMap, _ }

/**
  * Monotonic replay processor that uses a `concurrent.Map`
  * to hold temporary replay state, eventually persisting
  * to the [[delta.process.StreamProcessStore]].
  * Processing is done on threads partitioned by `SID`,
  * to ensure monotonic ordering and ''potentially'' better
  * L2+ cache utilization.
  *
  * @param persistentStore The persistent store
  * @param postReplayTimeout
  * @param writeBatchSize
  * @param partitionThreads
  * @param cmap
  */
abstract class IdempotentReplayProcessor[SID, EVT, S >: Null, U](
  protected val persistentStore: StreamProcessStore[SID, S, U],
  replayConfig: ReplayProcessConfig,
  cmap: CMap[SID, ReplayState[S]])
extends MonotonicReplayProcessor[SID, EVT, S, U](replayConfig)
with ConcurrentMapReplayPersistence[SID, EVT, S, U] {

  protected val processStore = ConcurrentMapStore.asReplayStore(cmap, persistentStore)

  protected def onReplayCompletion() = Future successful cmap

}

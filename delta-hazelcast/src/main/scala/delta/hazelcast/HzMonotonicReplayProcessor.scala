package delta.hazelcast

import delta.process._

import scala.concurrent._

import scuff.concurrent.{ PartitionedExecutionContext, Threads }

abstract class HzMonotonicReplayProcessor[SID, EVT, S >: Null, U](
  protected val persistentStore: StreamProcessStore[SID, S, U],
  replayConfig: ReplayProcessConfig,
  partitionThreads: PartitionedExecutionContext,
  cmap: collection.concurrent.Map[SID, ReplayState[S]])(
  implicit
  protected val completionContext: ExecutionContext)
extends MonotonicReplayProcessor[SID, EVT, S, U](replayConfig)
with ConcurrentMapReplayPersistence[SID, EVT, S, U] {

  def this(
      persistentStore: StreamProcessStore[SID, S, U],
      replayConfig: ReplayProcessConfig,
      replayfailureReporter: Throwable => Unit,
      localThreadCount: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[SID, ReplayState[S]] =
        new collection.concurrent.TrieMap[SID, ReplayState[S]])(
      implicit
      ec: ExecutionContext) =
    this(
      persistentStore, replayConfig,
      PartitionedExecutionContext(
          localThreadCount, replayConfig.maxBlockingQueueSize,
          Threads.newThreadGroup(s"${persistentStore.name}-replay-processor", daemon = false, replayfailureReporter)),
      cmap)

  protected val processStore = ConcurrentMapStore.asReplayStore(cmap, persistentStore)

  protected def processContext(id: SID): ExecutionContext = partitionThreads.singleThread(id.##)
  protected def onReplayCompletion(): Future[collection.concurrent.Map[SID, ReplayState]] =
    partitionThreads.shutdown().map(_ => cmap)

}

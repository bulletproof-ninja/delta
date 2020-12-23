package delta.hazelcast

import delta.process._

import scala.concurrent._
import scala.reflect.ClassTag

import scuff.concurrent.{ PartitionedExecutionContext, Threads }

abstract class HzMonotonicReplayProcessor[ID, EVT: ClassTag, S >: Null, U](
  protected val persistentStore: StreamProcessStore[ID, S, U],
  replayConfig: ReplayProcessConfig,
  partitionThreads: PartitionedExecutionContext,
  cmap: collection.concurrent.Map[ID, ConcurrentMapStore.State[S]])(
  implicit
  protected val executionContext: ExecutionContext)
extends MonotonicReplayProcessor[ID, EVT, S, U](replayConfig)
with ConcurrentMapReplayPersistence[ID, EVT, S, U] {

  def this(
      persistentStore: StreamProcessStore[ID, S, U],
      replayConfig: ReplayProcessConfig,
      replayfailureReporter: Throwable => Unit,
      localThreadCount: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[ID, ConcurrentMapStore.State[S]] =
        new collection.concurrent.TrieMap[ID, ConcurrentMapStore.State[S]])(
      implicit
      ec: ExecutionContext) =
    this(
      persistentStore, replayConfig,
      PartitionedExecutionContext(
          localThreadCount, replayfailureReporter,
          Threads.factory(s"${persistentStore.name}-replay-processor", replayfailureReporter)),
      cmap)

  override type Snapshot = delta.Snapshot[S]

  protected val processStore = ConcurrentMapStore.asReplayStore(cmap, persistentStore)

  protected def processContext(id: ID): ExecutionContext = partitionThreads.singleThread(id.##)
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, State]] =
    partitionThreads.shutdown().map(_ => cmap)

}

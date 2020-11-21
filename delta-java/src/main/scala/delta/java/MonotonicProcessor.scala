package delta.java

import scala.jdk.CollectionConverters._

import delta.{ Tick, Revision }
import delta.process.{ StreamProcessStore, ReplayProcessConfig }

import java.util.Optional

/** Monotonic processor. */
abstract class MonotonicProcessor[ID, EVT, S >: Null, U](
  protected val processStore: StreamProcessStore[ID, S, U])
extends delta.process.MonotonicProcessor[ID, EVT, S, U]
with LiveProcessor[ID, EVT]

/** Monotonic replay processor. */
abstract class MonotonicReplayProcessor[ID, EVT, S >: Null, U](
  protected val processStore: StreamProcessStore[ID, S, U],
  // protected val executionContext: ExecutionContext,
  protected val replayConfig: ReplayProcessConfig)
extends delta.process.MonotonicReplayProcessor[ID, EVT, S, U]
with ReplayProcessor[ID, EVT]

/** Monotonic processor with join state. */
abstract class JoinStateProcessor[ID, EVT, S >: Null, U](
  processStore: StreamProcessStore[ID, S, U])
extends MonotonicProcessor[ID, EVT, S, U](processStore)
with delta.process.MonotonicJoinState[ID, EVT, S, U]

/** Monotonic replay processor with join state. */
abstract class JoinStateReplayProcessor[ID, EVT, S >: Null, U](
  processStore: StreamProcessStore[ID, S, U],
  // ec: ExecutionContext,
  replayConfig: ReplayProcessConfig)
extends MonotonicReplayProcessor[ID, EVT, S, U](
  processStore, replayConfig)
with delta.process.MonotonicJoinState[ID, EVT, S, U] {

  protected def join(streamId: ID, streamRevision: Revision, tick: Tick, evt: EVT, metadata: Map[String, String]): Map[ID, Processor] = {
    joinEvent(streamId, streamRevision, tick, evt, metadata) match {
      case null => Map.empty
      case jmap => jmap.entrySet.iterator.asScala.foldLeft(Map.empty[ID, Processor]) {
        case (map, entry) => map.updated(entry.getKey, entry.getValue)
      }.toMap
    }
  }

  protected final def Processor(process: java.util.function.Function[Optional[S], S]): Processor = {
    val adapter = (state: Option[S]) => process(Optional.ofNullable(state.orNull))
    new Processor(adapter)
  }

  protected def joinEvent(streamId: ID, streamRevision: Revision, tick: Tick, evt: EVT, metadata: Map[String, String]): java.util.Map[ID, Processor]

}

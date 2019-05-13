package delta.java

import collection.JavaConverters._
import scala.concurrent.duration.{ FiniteDuration, TimeUnit }
import scala.reflect.ClassTag

import delta.process.StreamProcessStore
import java.util.Optional

/** Monotonic processor. */
abstract class MonotonicProcessor[ID, EVT, S >: Null](
    protected val processStore: StreamProcessStore[ID, S])
  extends delta.process.MonotonicProcessor[ID, EVT, S]
  with LiveProcessor[ID, EVT]

/** Monotonic replay processor. */
abstract class MonotonicReplayProcessor[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S],
    completionTimeout: Int, completionUnit: TimeUnit,
    evtClass: Class[_ <: EVT])
  extends delta.process.MonotonicReplayProcessor[ID, EVT, S, Object](
    new FiniteDuration(completionTimeout, completionUnit),
    processStore)(
    ClassTag(evtClass))
  with ReplayProcessor[ID, EVT]

/** Monotonic processor with join state. */
abstract class JoinStateProcessor[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S])
  extends MonotonicProcessor[ID, EVT, S](processStore)
  with delta.process.MonotonicJoinState[ID, EVT, S]

/** Monotonic replay processor with join state. */
abstract class JoinStateReplayProcessor[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S],
    completionTimeout: Int, completionUnit: TimeUnit,
    evtClass: Class[_ <: EVT])
  extends MonotonicReplayProcessor[ID, EVT, S](
    processStore, completionTimeout, completionUnit, evtClass)
  with delta.process.MonotonicJoinState[ID, EVT, S] {

  protected def join(streamId: ID, streamRevision: Int, tick: Long, evt: EVT, metadata: Map[String, String]): Map[ID, Processor] = {
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

  protected def joinEvent(streamId: ID, streamRevision: Int, tick: Long, evt: EVT, metadata: Map[String, String]): java.util.Map[ID, Processor]

}

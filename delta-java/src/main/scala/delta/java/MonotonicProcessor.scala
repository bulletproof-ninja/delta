package delta.java

import collection.JavaConverters._
import scala.concurrent.duration.{ FiniteDuration, TimeUnit }
import scala.reflect.ClassTag

import delta.util.StreamProcessStore
import java.util.Optional

/** Monotonic processor. */
abstract class MonotonicProcessor[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S],
    evtClass: Class[_ <: EVT])
  extends delta.util.MonotonicProcessor[ID, EVT, S](
    processStore)(
    ClassTag(evtClass))
  with LiveProcessor[ID, EVT]

/** Monotonic replay processor. */
abstract class MonotonicReplayProcessor[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S],
    completionTimeout: Int, completionUnit: TimeUnit,
    evtClass: Class[_ <: EVT])
  extends delta.util.MonotonicReplayProcessor[ID, EVT, S, Object](
    new FiniteDuration(completionTimeout, completionUnit),
    processStore)(
    ClassTag(evtClass))
  with ReplayProcessor[ID, EVT]

/** Monotonic processor with join state. */
abstract class JoinStateProcessor[ID, EVT, S >: Null, JS >: Null <: S](
    processStore: StreamProcessStore[ID, S],
    evtClass: Class[_ <: EVT])
  extends MonotonicProcessor[ID, EVT, S](
    processStore, evtClass)
  with delta.util.JoinStateProcessor[ID, EVT, S, JS]

/** Monotonic replay processor with join state. */
abstract class JoinStateReplayProcessor[ID, EVT, S >: Null, JS >: Null <: S](
    processStore: StreamProcessStore[ID, S],
    completionTimeout: Int, completionUnit: TimeUnit,
    evtClass: Class[_ <: EVT])
  extends MonotonicReplayProcessor[ID, EVT, S](
    processStore, completionTimeout, completionUnit, evtClass)
  with delta.util.JoinStateProcessor[ID, EVT, S, JS] {

  protected def preprocess(streamId: ID, streamRevision: Int, tick: Long, evt: EVT): Map[ID, Processor] = {
    preprocessEvent(streamId, streamRevision, tick, evt) match {
      case null => Map.empty
      case jmap => jmap.entrySet.iterator.asScala.foldLeft(Map.empty[ID, Processor]) {
        case (map, entry) => map.updated(entry.getKey, entry.getValue)
      }.toMap
    }
  }

  protected final def Processor(process: java.util.function.Function[Optional[JS], JS]): Processor = {
    val adapter = (state: Option[JS]) => process.apply(Optional.ofNullable(state.orNull))
    new Processor(adapter)
  }

  protected def preprocessEvent(streamId: ID, streamRevision: Int, tick: Long, evt: EVT): java.util.Map[ID, Processor]

}

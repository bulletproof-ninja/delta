package delta.java

import scala.concurrent.duration.{ FiniteDuration, TimeUnit }
import scala.reflect.ClassTag

import delta.util.StreamProcessStore

abstract class MonotonicProcessor[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S],
    evtClass: Class[_ <: EVT])
  extends delta.util.MonotonicProcessor[ID, EVT, S](
    processStore)(
    ClassTag(evtClass))
  with RealtimeProcessor[ID, EVT]

abstract class MonotonicBatchProcessor[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S],
    completionTimeout: Int, completionUnit: TimeUnit,
    evtClass: Class[_ <: EVT])
  extends delta.util.MonotonicBatchProcessor[ID, EVT, S, Object](
    new FiniteDuration(completionTimeout, completionUnit),
    processStore)(
    ClassTag(evtClass))
  with BatchProcessor[ID, EVT]

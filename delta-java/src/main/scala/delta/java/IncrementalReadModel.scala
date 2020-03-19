package delta.java

import java.util.concurrent.ScheduledExecutorService

import scala.reflect.ClassTag

import delta._
import delta.process.StreamProcessStore
import delta.MessageHub
import delta.process._

abstract class IncrementalReadModel[ID, ESID, EVT, Work >: Null, Stored, U](
  eventClass: Class[EVT],
  protected val processStore: StreamProcessStore[ESID, Stored, U],
  stateCodec: AsyncCodec[Work, Stored],
  protected val hub: MessageHub[ESID, Update[U]],
  protected val scheduler: ScheduledExecutorService)(
  eventSource: EventSource[ESID, _ >: EVT],
  idConv: ID => ESID)
extends delta.read.impl.IncrementalReadModel[ID, ESID, EVT, Work, Stored, U](eventSource)(
  ClassTag(eventClass), idConv, stateCodec)
with SubscriptionAdapter[ID, Stored, U]

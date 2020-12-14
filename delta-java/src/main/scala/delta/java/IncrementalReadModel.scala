package delta.java

import java.util.concurrent.ScheduledExecutorService

import scala.reflect.ClassTag

import delta._
import delta.process.StreamProcessStore
import delta.MessageHub
import delta.process._

abstract class IncrementalReadModel[ID, SID, EVT, Work >: Null, Stored, U](
  name: String,
  eventClass: Class[EVT],
  protected val processStore: StreamProcessStore[SID, Stored, U],
  stateCodec: AsyncCodec[Work, Stored],
  protected val hub: MessageHub[SID, delta.process.Update[U]],
  protected val scheduler: ScheduledExecutorService)(
  eventSource: EventSource[SID, _ >: EVT],
  idConv: ID => SID)
extends delta.read.impl.IncrementalReadModel[ID, SID, EVT, Work, Stored, U](name, eventSource)(
  ClassTag(eventClass), stateCodec, idConv)
with SubscriptionAdapter[ID, Stored, U]

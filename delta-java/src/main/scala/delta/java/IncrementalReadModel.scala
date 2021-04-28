package delta.java

import java.util.concurrent.ScheduledExecutorService

import delta._
import delta.process.StreamProcessStore
import delta.MessageHub
import delta.process._

abstract class IncrementalReadModel[ID, SID, EVT, InUse >: Null, AtRest, U](
  protected val processStore: StreamProcessStore[SID, AtRest, U],
  protected val hub: MessageHub[SID, Update[U]],
  protected val scheduler: ScheduledExecutorService)(
  eventSource: EventSource[SID, _ >: EVT],
  idConv: ID => SID)
extends delta.read.impl.FlexIncrementalReadModel[ID, SID, EVT, InUse, AtRest, U](eventSource)(idConv)
with SubscriptionAdapter[ID, AtRest, U]

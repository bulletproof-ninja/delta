package delta.java

import delta.MessageHub
import delta.read._
import delta.process.Update
import scala.concurrent.duration._
import java.util.concurrent.ScheduledExecutorService

abstract class PrebuiltReadModel[ID, SID, S, U](
  name: String,
  defaultReadTimeout: FiniteDuration,
  protected val hub: MessageHub[SID, Update[U]],
  scheduler: ScheduledExecutorService)(
  implicit
  idConv: ID => SID)
extends impl.FlexPrebuiltReadModel[ID, SID, S, U](
  name,
  defaultReadTimeout)(
  idConv,
  scheduler)
with MessageHubSupport[ID, S, U]
with SubscriptionAdapter[ID, S, U] {

  def this(
      name: String,
      defaultLookupTimeoutLength: Long, defaultLookupTimeoutUnits: TimeUnit,
      hub: MessageHub[SID, Update[U]],
      scheduler: ScheduledExecutorService)(
      implicit
      idConv: ID => SID) =
    this(name, FiniteDuration(defaultLookupTimeoutLength, defaultLookupTimeoutUnits), hub, scheduler)

  def this(
      name: String,
      hub: MessageHub[SID, Update[U]],
      scheduler: ScheduledExecutorService)(
      implicit
      idConv: ID => SID) =
    this(name, DefaultReadTimeout, hub, scheduler)

}

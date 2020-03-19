package delta.java

import delta.MessageHub
import delta.read._
import delta.process.Update
import scala.concurrent.duration._
import java.util.concurrent.ScheduledExecutorService

abstract class PrebuiltReadModel[ID, S, MHID, U](
  defaultReadTimeout: FiniteDuration,
  protected val hub: MessageHub[MHID, Update[U]],
  protected val scheduler: ScheduledExecutorService)(
  implicit
  idConv: ID => MHID)
extends impl.PrebuiltReadModel[ID, S, MHID, U](defaultReadTimeout)
with MessageHubSupport[ID, S, U]
with SubscriptionAdapter[ID, S, U] {

  def this(
      defaultLookupTimeoutLength: Long, defaultLookupTimeoutUnits: TimeUnit,
      hub: MessageHub[MHID, Update[U]],
      scheduler: ScheduledExecutorService)(
      implicit
      idConv: ID => MHID) =
    this(FiniteDuration(defaultLookupTimeoutLength, defaultLookupTimeoutUnits), hub, scheduler)

  def this(
      hub: MessageHub[MHID, Update[U]],
      scheduler: ScheduledExecutorService)(
      implicit
      idConv: ID => MHID) =
    this(DefaultReadTimeout, hub, scheduler)

}

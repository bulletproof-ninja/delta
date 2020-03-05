package delta.java

import delta.MessageHub
import delta.read._
import scala.concurrent.duration._
import java.util.concurrent.ScheduledExecutorService

abstract class PrebuiltReadModel[ID, MHID, S, U](
  defaultReadTimeout: FiniteDuration,
  protected val snapshotHub: MessageHub,
  protected val snapshotTopic: MessageHub.Topic,
  protected val scheduler: ScheduledExecutorService)(
  implicit
  idConv: ID => MHID)
extends impl.PrebuiltReadModel[ID, S, U](defaultReadTimeout)
with delta.read.MessageHubSupport[ID, S, U]
with SubscriptionAdapter[ID, S, U] {

  def this(
      defaultLookupTimeoutLength: Long, defaultLookupTimeoutUnits: TimeUnit,
      hub: MessageHub,
      hubNS: MessageHub.Topic,
      scheduler: ScheduledExecutorService)(
      implicit
      idConv: ID => MHID) =
    this(FiniteDuration(defaultLookupTimeoutLength, defaultLookupTimeoutUnits), hub, hubNS, scheduler)

  def this(
      hub: MessageHub,
      hubNS: MessageHub.Topic,
      scheduler: ScheduledExecutorService)(
      implicit
      idConv: ID => MHID) =
    this(DefaultReadTimeout, hub, hubNS, scheduler)

}

package delta.java

import delta.SnapshotStore
import delta.MessageHub
import delta.read._
import scala.concurrent.duration._
import java.util.concurrent.ScheduledExecutorService
import scuff.concurrent.Threads
import scala.reflect.ClassTag

abstract class PrebuiltReadModel[ID, SSID, SS, S >: SS](
    stateClass: Class[S],
    snapshotStore: SnapshotStore[SSID, SS],
    defaultReadTimeout: FiniteDuration,
    protected val snapshotHub: MessageHub,
    protected val snapshotTopic: MessageHub.Topic,
    scheduler: ScheduledExecutorService)(
    implicit
    idConv: ID => SSID)
  extends impl.PrebuiltReadModel[ID, SSID, SS, S](
    snapshotStore, scheduler, defaultReadTimeout)(
    ClassTag(stateClass), idConv)
  with delta.read.MessageHubSupport[ID, SSID, S]
  with SubscriptionAdapter[ID, S] {

  def this(
      stateClass: Class[S],
      store: SnapshotStore[SSID, SS],
      defaultLookupTimeoutLength: Long, defaultLookupTimeoutUnits: TimeUnit,
      hub: MessageHub,
      hubNS: MessageHub.Topic,
      scheduler: ScheduledExecutorService)(
      implicit
      idConv: ID => SSID) =
    this(stateClass, store, FiniteDuration(defaultLookupTimeoutLength, defaultLookupTimeoutUnits), hub, hubNS, scheduler)

  def this(
      stateClass: Class[S],
      store: SnapshotStore[SSID, SS],
      hub: MessageHub,
      hubNS: MessageHub.Topic)(
      implicit
      idConv: ID => SSID) =
    this(stateClass, store, DefaultReadTimeout, hub, hubNS, Threads.DefaultScheduler)

  def this(
      stateClass: Class[S],
      store: SnapshotStore[SSID, SS],
      hub: MessageHub,
      hubNS: MessageHub.Topic,
      scheduler: ScheduledExecutorService)(
      implicit
      idConv: ID => SSID) =
    this(stateClass, store, DefaultReadTimeout, hub, hubNS, scheduler)

}

package delta.java

import java.util.concurrent.ScheduledExecutorService

import scala.reflect.ClassTag

import delta.{ EventSource, Projector }
import delta.process.StreamProcessStore
import delta.MessageHub

abstract class IncrementalReadModel[ID, ESID, S >: Null, EVT] private (
    snapshotClass: Class[S], eventClass: Class[EVT],
    projectorSource: Either[Map[String, String] => Projector[S, EVT], Projector[S, EVT]],
    protected val processStore: StreamProcessStore[ESID, S],
    protected val snapshotHub: delta.MessageHub,
    protected val snapshotTopic: delta.MessageHub.Topic,
    protected val scheduler: ScheduledExecutorService)(
    eventSource: EventSource[ESID, _ >: EVT],
    idConv: ID => ESID)

  extends delta.read.impl.IncrementalReadModel[ID, ESID, S, EVT](projectorSource, eventSource)(
    ClassTag(snapshotClass), ClassTag(eventClass), idConv)
  with SubscriptionAdapter[ID, S] {

  def this(
      idConv: ID => ESID,
      snapshotClass: Class[S], eventClass: Class[EVT],
      projector: Projector[S, EVT],
      processStore: StreamProcessStore[ESID, S],
      snapshotHub: MessageHub,
      snapshotTopic: String,
      scheduler: ScheduledExecutorService,
      eventSource: EventSource[ESID, _ >: EVT]) =
    this(
      snapshotClass, eventClass, Right(projector), processStore,
      snapshotHub, MessageHub.Topic(snapshotTopic), scheduler)(
      eventSource, idConv)

  def this(
      idConv: ID => ESID,
      snapshotClass: Class[S], eventClass: Class[EVT],
      withMetadata: Map[String, String] => Projector[S, EVT],
      processStore: StreamProcessStore[ESID, S],
      snapshotHub: MessageHub,
      snapshotTopic: String,
      scheduler: ScheduledExecutorService,
      eventSource: EventSource[ESID, _ >: EVT]) =
    this(
      snapshotClass, eventClass, Left(withMetadata), processStore,
      snapshotHub, MessageHub.Topic(snapshotTopic), scheduler)(
      eventSource, idConv)

}

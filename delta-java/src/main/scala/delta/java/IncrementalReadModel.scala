package delta.java

import java.util.concurrent.ScheduledExecutorService

import scala.reflect.ClassTag

import delta._
import delta.process.StreamProcessStore
import delta.MessageHub
import scuff.Codec
import delta.process.AsyncCodec

abstract class IncrementalReadModel[ID, ESID, EVT, Work >: Null, Stored, U](
  eventClass: Class[EVT],
  protected val processStore: StreamProcessStore[ESID, Stored, U],
  stateCodec: AsyncCodec[Work, Stored],
  protected val hub: delta.MessageHub,
  protected val hubTopic: delta.MessageHub.Topic,
  protected val scheduler: ScheduledExecutorService)(
  eventSource: EventSource[ESID, _ >: EVT],
  idCodec: Codec[ESID, ID])
extends delta.read.impl.IncrementalReadModel[ID, ESID, EVT, Work, Stored, U](eventSource)(
  ClassTag(eventClass), idCodec, stateCodec)
with SubscriptionAdapter[ID, Stored, U] {

  def this(
      eventClass: Class[EVT],
      processStore: StreamProcessStore[ESID, Stored, U],
      stateCodec: AsyncCodec[Work, Stored],
      hub: MessageHub,
      hubTopic: String,
      scheduler: ScheduledExecutorService,
      eventSource: EventSource[ESID, _ >: EVT],
      idCodec: Codec[ESID, ID]) =
    this(
      eventClass,
      processStore, stateCodec,
      hub, MessageHub.Topic(hubTopic), scheduler)(
      eventSource, idCodec)

}

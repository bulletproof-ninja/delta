package delta.read.impl

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import delta.process._
import scala.reflect.ClassTag
import scuff.Subscription
import delta.read._
import delta._

/**
 * Incrementally built on-demand read-model, pull or push.
 * Uses a [[delta.util.StreamProcessStore]] for
 * storing state and subscribes to a [[delta.EventSource]]
 * for any new events, and updates the process store
 * if anything changed.
 * NOTE: *Cannot* be used for state derived
 * from joined streams.
 * @tparam ID The id type
 * @tparam S The model type
 * @tparam EVT The event type used for producing state
 */
abstract class IncrementalReadModel[ID, ESID, S >: Null: ClassTag, EVT: ClassTag](
    txProjector: TransactionProjector[S, EVT],
    es: EventSource[ESID, _ >: EVT])(
    implicit
    convId: ID => ESID)
  extends EventSourceReadModel[ID, ESID, S, EVT](es, txProjector)
  with SubscriptionSupport[ID, S]
  with MessageHubSupport[ID, ESID, S]
  with ProcessStoreSupport[ID, ESID, S] {

  def this(
      projector: Projector[S, EVT],
      eventSource: EventSource[ESID, _ >: EVT])(
      implicit
      convId: ID => ESID) =
    this(TransactionProjector[S, EVT](projector), eventSource)

  protected def readStrict(id: ID, expected: Either[Long, Int])(
      implicit
      ec: ExecutionContext): Future[Snapshot] = {
    expected match {
      case Right(minRevision) =>
        readAndUpdate(id, minRev = minRevision).flatMap(verifyRevision(id, _, minRevision))
      case Left(minTick) =>
        readAndUpdate(id, minTick = minTick).flatMap(verifyTick(id, _, minTick))
    }
  }

  protected def idConv(id: ID): ESID = convId(id)

  private[this] val started = new AtomicBoolean(false)
  def isStarted: Boolean = started.get

  protected def defaultReadTimeout = DefaultReadTimeout

  /**
   * Start processing.
   * NOTE: If the process store is a centralized database (normal)
   * and there are multiple instances of a given read model (normal,
   * if load-balanced/clustered/HA), make sure only one is started,
   * to avoid unnecessary processing and database access.
   * @throws IllegalStateException if attempt to start more than once
   */
  protected def start(selector: eventSource.Selector): Subscription = {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Already started!")
    }
    eventSource.subscribe(selector.toStreamsSelector)(onTxnUpdate)
  }

  def readLatest(id: ID)(implicit ec: ExecutionContext): Future[Snapshot] = {
    val esid = idConv(id)
    processStore.read(esid).flatMap {
      case Some(snapshot) => Future successful snapshot
      case _ => // id was not found, so read and update manually
        readAndUpdate(id).flatMap {
          verify(id, _)
        }
    }
  }

  def readMinRevision(id: ID, minRevision: Int)(implicit ec: ExecutionContext): Future[Snapshot] =
    readMinRevision(id, minRevision, DefaultReadTimeout)

  def readMinTick(id: ID, minTick: Long)(implicit ec: ExecutionContext): Future[Snapshot] =
    readMinTick(id, minTick, DefaultReadTimeout)

  protected def replayDelayOnMissing: FiniteDuration = 2.seconds

  private[this] val onTxnUpdate = new MonotonicProcessor[ESID, EVT, S] with MissingRevisionsReplay[ESID, EVT] {
    protected def onMissingRevisions(id: ESID, missing: Range): Unit =
      replayMissingRevisions(
        eventSource, replayDelayOnMissing, scheduler, processContext(id).reportFailure)(
        id, missing)(this.apply)
    protected def onSnapshotUpdate(id: ESID, update: SnapshotUpdate): Unit =
      snapshotHub.publish(snapshotTopic, id -> update)
    protected def processStore = IncrementalReadModel.this.processStore
    protected def processContext(id: ESID) = IncrementalReadModel.this.processContext(id)
    protected def process(txn: TXN, currState: Option[S]): Future[S] = txProjector(txn, currState)
      
  }

}

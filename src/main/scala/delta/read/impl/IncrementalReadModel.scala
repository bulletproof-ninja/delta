package delta.read.impl

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent._
import scala.reflect.ClassTag

import delta._
import delta.process._
import delta.read._

/**
 * Incrementally built on-demand read-model, pull or push.
 * Uses a [[delta.process.StreamProcessStore]] for
 * storing state and subscribes to a [[delta.EventSource]]
 * for any new events, and updates the process store
 * if anything changed, subsequntly publishing those changes
 * over the [[delta.MessageHub]].
 * @note ''Cannot'' be used for state derived
 * from joined streams.
 * @tparam ID The id type
 * @tparam SID The stream id
 * @tparam EVT The event type
 * @tparam InUse The in-memory state type for projection updates
 * @tparam AtRest The storage state type
 * @tparam U The update state type
 */
private[impl] abstract class BaseIncrementalReadModel[ID, SID, EVT, InUse >: Null, AtRest, U](
  es: EventSource[SID, _ >: EVT])(
  implicit
  convId: ID => SID)
extends EventSourceReadModel[ID, SID, EVT, InUse, AtRest](es)
with MessageHubSupport[ID, AtRest, U]
with ProcessStoreSupport[ID, SID, InUse, AtRest, U] {

  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] =
    (processStore read id).flatMap {
      case None => readAndUpdate(id)
      case some => Future successful some
    }

  protected def readAgain(id: ID, minRevision: Revision, minTick: Tick)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] =
    readAndUpdate(id, minRevision, minTick)

  private[this] val started = new AtomicBoolean(false)
  def isStarted: Boolean = started.get

  protected def defaultReadTimeout = DefaultReadTimeout

  /**
   * Start processing.
   * @note If the process store is a centralized database (normal)
   * and there are multiple instances of a given read model (normal,
   * if load-balanced/clustered/HA), make sure only one is started,
   * to avoid unnecessary processing and database access.
   */
  @throws[IllegalStateException]("if attempt to start more than once")
  protected def start(selector: eventSource.Selector): LiveProcess = {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Already started!")
    }
    val subscription = eventSource.subscribeLocal(selector.toStreamsSelector)(onTxUpdate)
    new LiveProcess {
      def name = BaseIncrementalReadModel.this.name
      def cancel() = subscription.cancel()
    }
  }

  protected def onMissingRevision: LiveProcessConfig.OnMissingRevision

  private[this] val onTxUpdate =
    new MonotonicProcessor[SID, EVT, InUse, U]
    with MissingRevisionsReplay[SID, EVT] {

      def name = processStore.name

      protected def onMissingRevisions(id: SID, missing: Range): Unit =
        replayMissingRevisions(
          eventSource, onMissingRevision)(
          id, missing)(this.apply)

      protected def onUpdate(id: SID, update: Update): Unit =
        updateHub.publish(id, update)

      protected val processStore = BaseIncrementalReadModel.this.processStore.adaptState(stateAugmentCodec _)
      protected def processContext(id: SID) = BaseIncrementalReadModel.this.processContext(id)

      private[this] val projector = Projector(BaseIncrementalReadModel.this.projector) _
      protected def process(tx: Transaction, currState: Option[InUse]): Future[InUse] =
        projector(tx, currState)

  }

}

/**
  * Simple incremental read model.
  */
abstract class IncrementalReadModel[ID, SID, EVT, S >: Null](
  es: EventSource[SID, _ >: EVT])(
  implicit
  convId: ID => SID)
extends BaseIncrementalReadModel[ID, SID, EVT, S, S, S](es) {

  protected def stateAugmentCodec(id: SID) = AsyncCodec.noop[S]
  protected def updateState(id: ID, prevState: Option[S], currState: S) = Some(currState)

}

/**
  * Flexible incremental read model, where the model type needs augmenting
  * before being stored, and/or the published updates are diffs rather than
  * full state.
  */
abstract class FlexIncrementalReadModel[ID, SID, EVT, InUse >: Null, AtRest, U](
  es: EventSource[SID, _ >: EVT])(
  implicit
  convId: ID => SID)
extends BaseIncrementalReadModel[ID, SID, EVT, InUse, AtRest, U](es)

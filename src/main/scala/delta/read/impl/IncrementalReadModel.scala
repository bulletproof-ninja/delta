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
 * @tparam Work The model type, for projection updates
 * @tparam Stored The model type, for storage
 * @tparam U The update model
 */
abstract class IncrementalReadModel[ID, SID, EVT: ClassTag, Work >: Null, Stored, U](
  protected val name: String,
  es: EventSource[SID, _ >: EVT])(
  implicit
  stateCodec: AsyncCodec[Work, Stored],
  convId: ID => SID)
extends EventSourceReadModel[ID, SID, EVT, Work, Stored](es)
with MessageHubSupport[ID, Stored, U]
with ProcessStoreSupport[ID, SID, Work, Stored, U] {

  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] =
    (processStore read id).flatMap {
      case None => readAndUpdate(id)
      case some => Future successful some
    }

  protected override def readAgain(id: ID, minRevision: Revision, minTick: Tick)(
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
  protected def start(selector: eventSource.Selector): Subscription = {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Already started!")
    }
    eventSource.subscribe(selector.toStreamsSelector)(onTxUpdate)
  }

  protected def replayDelayOnMissing: FiniteDuration = 2.seconds

  private[this] val onTxUpdate =
    new MonotonicProcessor[SID, EVT, Work, U]
    with MissingRevisionsReplay[SID, EVT] {

      protected def onMissingRevisions(id: SID, missing: Range): Unit =
        replayMissingRevisions(
          eventSource, Some(replayDelayOnMissing -> scheduler))(
          id, missing)(this.apply)

      protected def onUpdate(id: SID, update: Update): Unit =
        hub.publish(id, update)

      protected val processStore = IncrementalReadModel.this.processStore.adaptState(stateCodec, stateCodecContext)
      protected def processContext(id: SID) = IncrementalReadModel.this.processContext(id)

      private[this] val projector = Projector(IncrementalReadModel.this.projector) _
      protected def process(tx: Transaction, currState: Option[Work]): Future[Work] =
        projector(tx, currState)

  }

}

/**
  * Model where the various state representations are identical.
  */
abstract class SimpleIncrementalReadModel[ID, SID, EVT: ClassTag, S >: Null](
  name: String,
  es: EventSource[SID, _ >: EVT])(
  implicit
  convId: ID => SID)
extends IncrementalReadModel[ID, SID, EVT, S, S, S](name, es) {

  protected def stateCodecContext = scuff.concurrent.Threads.PiggyBack
  protected def updateState(id: ID, prevState: Option[S], currState: S) = Some(currState)

}

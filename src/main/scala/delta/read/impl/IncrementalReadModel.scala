package delta.read.impl

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import delta.process._, AsyncCodec.noop
import scala.reflect.ClassTag
import scuff.Subscription
import delta.read._
import delta._
import scuff.Codec

/**
 * Incrementally built on-demand read-model, pull or push.
 * Uses a [[delta.process.StreamProcessStore]] for
 * storing state and subscribes to a [[delta.EventSource]]
 * for any new events, and updates the process store
 * if anything changed.
 * NOTE: *Cannot* be used for state derived
 * from joined streams.
 * @tparam ID The id type
 * @tparam ESID The event source id
 * @tparam EVT The event type
 * @tparam Work The model type, for projection updates
 * @tparam Stored The model type, for storage
 * @tparam U The update model
 */
abstract class IncrementalReadModel[ID, ESID, EVT: ClassTag, Work >: Null, Stored, U](
  es: EventSource[ESID, _ >: EVT])(
  implicit
  idCodec: Codec[ESID, ID],
  protected val stateCodec: AsyncCodec[Work, Stored])
extends EventSourceReadModel[ID, ESID, EVT, Work, Stored](es)
with SubscriptionSupport[ID, Stored, U]
with MessageHubSupport[ID, Stored, U]
with ProcessStoreSupport[ID, ESID, Work, Stored, U] {

  protected val idConv = (id: ID) => idCodec decode id

  protected def readAgain(id: ID, minRevision: Int, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] =
    readAndUpdate(id, minRevision, minTick)

  private[this] val started = new AtomicBoolean(false)
  def isStarted: Boolean = started.get

  protected def defaultReadTimeout = DefaultReadTimeout

  /**
   * Start processing.
   * NOTE: If the process store is a centralized database (normal)
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

  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] = processStore read (idCodec decode id)

  def read(id: ID, minRevision: Int)(implicit ec: ExecutionContext): Future[Snapshot] =
    read(id, minRevision, DefaultReadTimeout)

  def read(id: ID, minTick: Long)(implicit ec: ExecutionContext): Future[Snapshot] =
    read(id, minTick, DefaultReadTimeout)

  protected def replayDelayOnMissing: FiniteDuration = 2.seconds

  private[this] val onTxUpdate =
    new MonotonicProcessor[ESID, EVT, Work, U]
    with MissingRevisionsReplay[ESID, EVT] {

      protected def onMissingRevisions(id: ESID, missing: Range): Unit =
        replayMissingRevisions(
          eventSource, replayDelayOnMissing, scheduler, processContext(id).reportFailure)(
          id, missing)(this.apply)

      protected def onUpdate(id: ESID, update: Update): Unit = {
        hub.publish(hubTopic, idCodec.encode(id) -> update)
      }

      protected val processStore = IncrementalReadModel.this.processStore.adaptState(stateCodec, stateCodecContext)
      protected def processContext(id: ESID) = IncrementalReadModel.this.processContext(id)

      private[this] val projector = Projector(IncrementalReadModel.this.projector) _
      protected def process(tx: Transaction, currState: Option[Work]): Future[Work] =
        projector(tx, currState)

  }

}

/**
  * Model where the various state representations are identical.
  */
abstract class PlainIncrementalReadModel[ID, ESID, EVT: ClassTag, S >: Null](
  es: EventSource[ESID, _ >: EVT])(
  implicit
  idCodec: Codec[ESID, ID])
extends IncrementalReadModel[ID, ESID, EVT, S, S, S](es) {
  import scuff.concurrent.Threads

  protected def updateState(id: ID,prevState: Option[S],update: S): Option[S] = Option(update)
  protected def stateCodecContext = Threads.PiggyBack

}

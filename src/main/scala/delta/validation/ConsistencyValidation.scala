package delta.validation

import delta._
import scala.concurrent._
import scala.collection.concurrent.TrieMap
import scala.util.{ Success, Failure }
import delta.process.ReplayProcess
import delta.process.ReplayResult

/**
 * Apply this trait to the [[delta.EventStore]] `with` [[delta.TransactionPublishing]]
 * for global consistency validation.
 *
 * This will delay publishing of a given transaction that has lead to
 * inconsistent state until that transaction has been compensated by
 * another transaction. This can prevent downstream consumers from exposing
 * inconsistent state, even briefly.
 * @note The [[delta.EventStore]] that this trait is applied to ''must''
 * have validation process activated by calling the `activate` method.
 */
trait ConsistencyValidation[SID, EVT]
extends TransactionPublishing[SID, EVT] {

  private abstract class Activation {
    type State
    def txProcessor: Transaction => Future[State]
    def compensation: PartialFunction[Channel, Compensation[SID, State]]
  }

  private[this] val activation = Promise[Activation]()
  private[this] def activate[S](
      processor: Transaction => Future[S],
      comp: PartialFunction[Channel, Compensation[SID, S]])
      : Unit =
    try
      activation success new Activation {
        type State = S
        def txProcessor = processor
        def compensation = comp
      }
    catch {
      case _: IllegalStateException =>
        throw new IllegalStateException(s"Already activated for validation!")
    }

  /** Activate this validation. */
  def activate[S](
      validation: EventStoreValidationProcess[SID, _ <: EVT, S],
      config: delta.process.LiveProcessConfig)
      : Unit =
    activate(
      validation.txProcessor(this, config),
      validation.compensation)

  /**
    * Activate this validation, and also validate any outstanding
    * transactions.
    * @note Should generally only be done from a single instance
    * at startup, but if the compensating commands are idempotent,
    * it should be safe to do in general.
    */
  def activateAndValidate[S](
      validation: EventStoreValidationProcess[SID, _ <: EVT, S],
      replayConfig: delta.process.ReplayProcessConfig,
      liveConfig: delta.process.LiveProcessConfig)(
      implicit
      ec: ExecutionContext)
      : ReplayProcess[ReplayResult[SID]] = {

    val txProcessor = validation.txProcessor(this, liveConfig)

    activate(txProcessor, validation.compensation)

    val replayProc = validation.validate(this, replayConfig)
    val streamCompletion =
      replayProc.finished
        .flatMap { replayCompletion =>
          validation.completeStreams(this, replayCompletion.incompleteStreams, txProcessor)
            .map { streamErrors =>
              ReplayResult(replayCompletion.txCount, streamErrors)
            }
        }
    ReplayProcess(replayProc, streamCompletion)
  }

  // While it's unlikely that there'll be multiple concurrent
  // compensating transactions for the same stream,
  // it's not impossible.
  private case class StreamStatus(
      active: Set[Revision],
      alreadyPublished: Set[Revision],
      // NOTE: Keyed by compensating revision (NOT own revision):
      delayedPublishing: Map[Revision, Transaction]) {
    def this(curr: Revision) = this(Set(curr), Set.empty, Map.empty)
  }

  private[this] object StreamStatus {
    private[this] val byStream = new TrieMap[SID, StreamStatus]

    def activate(tx: Transaction, existingStatus: StreamStatus = null): StreamStatus = {

      val status =
        if (existingStatus == null) new StreamStatus(tx.revision)
        else existingStatus.copy(active = existingStatus.active + tx.revision)

      if (existingStatus == null) byStream.putIfAbsent(tx.stream, status) match {
        case None => status
        case Some(alreadyActive) =>
          activate(tx, alreadyActive)
      } else if (byStream.replace(tx.stream, existingStatus, status)) {
        status
      } else {
        val alreadyActive = byStream.getOrElse(tx.stream, null)
        activate(tx, alreadyActive)
      }

    }

    /**
     * Deactivate published transaction.
     * @return Delayed transaction, if exists
     */
    def deactivate(
        publishedTx: Transaction,
        status: StreamStatus): Option[(Transaction, StreamStatus)] =

      if (status.delayedPublishing.isEmpty &&
          status.active.size == 1) { // Fast path:

        assert(status.active contains publishedTx.revision)

        if (byStream.remove(publishedTx.stream, status)) None
        else byStream.get(publishedTx.stream) match {
          case Some(status) => deactivate(publishedTx, status)
          case None => None
        }

      } else { // Slow path (has delayed transactions or other active txs):

        val txToPublish = status.delayedPublishing get publishedTx.revision
        val updStatus = {
          if (txToPublish.isEmpty) status.copy(
            active = status.active - publishedTx.revision,
            alreadyPublished = status.alreadyPublished + publishedTx.revision)
          else status.copy(
            active = status.active - publishedTx.revision,
            alreadyPublished = status.alreadyPublished + publishedTx.revision,
            delayedPublishing = status.delayedPublishing - publishedTx.revision)
        }

        if (byStream.replace(publishedTx.stream, status, updStatus)) {
           txToPublish.map(_ -> updStatus)
        } else {
          val status = byStream.getOrElseUpdate(publishedTx.stream, updStatus)
          deactivate(publishedTx, status)
        }

      }

    /**
     * Delay transaction publishing.
     * @return `None` for successful delay,
     * or `Some` if the `waitFor` revision has already been published.
     */
    def delay(tx: Transaction, waitFor: Revision, status: StreamStatus): Option[StreamStatus] = {
      if (status.alreadyPublished contains waitFor) Some(status)
      else {
        val updStatus = status.copy(
            delayedPublishing = status.delayedPublishing.updated(waitFor, tx))
        if (byStream.replace(tx.stream, status, updStatus)) {
          None
        } else {
          val status = byStream.getOrElseUpdate(tx.stream, new StreamStatus(tx.revision))
          delay(tx, waitFor, status)
        }
      }
    }
  }

  protected def validationContext(stream: SID): ExecutionContext

  abstract final override protected def publishTransaction(
      stream: SID, ch: Channel, txFuture: Future[Transaction]): Unit = {
    implicit val ec = validationContext(stream)
    for (a <- this.activation.future) {
      if (a.compensation isDefinedAt ch) txFuture.foreach {
        publishOrDelay(_, a.txProcessor, a.compensation(ch))
      } else {
        super.publishTransaction(stream, ch, txFuture)
      }
    }

  }

  private def publishOrDelay[S](
      tx: Transaction,
      txProcessor: Transaction => Future[S],
      compensation: Compensation[SID, S])(
      implicit
      ec: ExecutionContext): Unit = {

    val status = StreamStatus.activate(tx)
    publishOrDelay(tx, txProcessor, compensation, status)
      .failed.foreach(ec.reportFailure)

  }

  private def publishOrDelay[S](
      tx: Transaction,
      txProcessor: Transaction => Future[S],
      compensate: Compensation[SID, S],
      status: StreamStatus)(
      implicit
      ec: ExecutionContext): Future[Unit] =

    for {
      state <- txProcessor(tx)
      compensatingTransactions <- compensate.ifNeeded(tx.stream, Snapshot(state, tx.revision, tx.tick))
    } yield {
      var isCurrTxCompensated = false
      compensatingTransactions.foreach {
        case (tx.stream, Success(revision)) =>
          isCurrTxCompensated = true
          StreamStatus.delay(tx, revision, status) match {
            case None => // Delay successful
            case Some(status) => // Compensating tx already published, so don't delay
              assert(status.alreadyPublished contains revision)
              publishAndDeactivate(tx, status)
          }
        case (_, Success(_)) => // Other stream had compensating action taken
          // TODO: Delay in this case as well? Probably not, but hard to say in general.
        case (_, Failure(cause)) => // Compensating action failed.
          // TODO: Can we do more?
          ec reportFailure cause
      }
      if (!isCurrTxCompensated) {
        publishAndDeactivate(tx, status)
      }
    }

  private def publishAndDeactivate(tx: Transaction, status: StreamStatus): Unit = {
    try super.publishTransaction(tx.stream, tx.channel, Future successful tx) finally {
      StreamStatus.deactivate(tx, status).foreach {
        case (anotherTx, updStatus) => publishAndDeactivate(anotherTx, updStatus)
      }
    }
  }

}

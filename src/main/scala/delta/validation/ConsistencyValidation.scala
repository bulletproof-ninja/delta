package delta.validation

import delta._
import scala.concurrent._
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.IntMap

/**
 * Consistency validation that delays publishing of
 * transactions until compensating transactions have
 * been broadcast.
 * This prevents downstream consumers from exposing
 * inconsistent state, even briefly.
 * @note The event store this trait is applied to _must_
 * have validation activated by calling the `activate`
 * method.
 */
trait ConsistencyValidation[SID, EVT]
extends TransactionPublishing[SID, EVT] {

  private abstract class Activation {
    type State
    def txProcessor: Transaction => Future[State]
    def compensation(ch: Channel): Option[Compensation[SID, State]]
  }

  private[this] val activation = Promise[Activation]

  def activate[S](
      validation: EventStoreValidationProcess[SID, _ <: EVT, S])
      : Unit =
    try
      activation success new Activation {
        type State = S
        val txProcessor = validation.txProcessor(ConsistencyValidation.this)
        def compensation(ch: Channel) = validation.compensation(ch)
      }
    catch {
      case _: IllegalStateException =>
        throw new IllegalStateException(s"Already activated for validation!")
    }

  // While it's unlikely that there'll be multiple concurrent
  // compensating transactions for the same stream,
  // it's not impossible.
  private case class StreamStatus(
      active: Set[Revision],
      alreadyPublished: Set[Revision],
      // NOTE: Keyed by compensating revision (NOT own revision):
      delayedPublishing: IntMap[Transaction]) {
    def this(curr: Revision) = this(Set(curr), Set.empty, IntMap.empty)
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
    for (activation <- this.activation.future) {
      activation.compensation(ch) match {
        case None =>
          super.publishTransaction(stream, ch, txFuture)
        case Some(compensation) =>
          txFuture.foreach {
            publishOrDelay(_, activation.txProcessor, compensation)
          }
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
      outcome <- compensate.ifNeeded(tx.stream, tx.tick, state)
    } yield outcome match {
      case NotNeeded =>
        publishAndDeactivate(tx, status)
      case Compensated(revision) =>
        StreamStatus.delay(tx, revision, status) match {
          case None => // 👍
          case Some(status) =>
            assert(status.alreadyPublished contains revision)
            publishAndDeactivate(tx, status)
        }
    }

  private def publishAndDeactivate(tx: Transaction, status: StreamStatus): Unit = {
    try super.publishTransaction(tx.stream, tx.channel, Future successful tx) finally {
      StreamStatus.deactivate(tx, status).foreach {
        case (tx, status) => publishAndDeactivate(tx, status)
      }
    }
  }

}

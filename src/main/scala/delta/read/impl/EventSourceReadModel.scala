package delta.read.impl

import delta.process.AsyncCodec

import scuff.concurrent._

import scala.concurrent._

import delta._
import delta.read._
import scuff.Reduction


private[impl]
abstract class EventSourceReadModel[ID, SID, EVT, InUse >: Null, AtRest] protected (
  protected val eventSource: EventSource[SID, _ >: EVT])(
  implicit
  idConv: ID => SID)
extends ReadModel[ID, AtRest]
with StreamId {

  protected type StreamId = SID
  protected def StreamId(id: ID) = idConv(id)
  protected type Transaction = delta.Transaction[SID, _ >: EVT]
  protected type InUseSnapshot = delta.Snapshot[InUse]

  protected def projector(tx: Transaction): Projector[InUse, EVT]

  /**
    * Optional codec for augmenting state before being stored.
    * @note This is not common and if not needed, simply return `AsyncCodec.noop`.
    * @param id
    * @return
    */
  protected def stateAugmentCodec(id: SID): AsyncCodec[InUse, AtRest]

  protected def replayToComplete(
      snapshot: Option[Snapshot], id: SID)
      : Future[Option[Snapshot]] = {

    val projector = Projector(this.projector) _

    class SnapshotFold(wSnapshot: Option[InUseSnapshot])
    extends Reduction[Transaction, Future[Option[InUseSnapshot]]] {
      private[this] var revision: Revision = wSnapshot.map(_.revision) getOrElse -1
      private[this] var tick: Tick = wSnapshot.map(_.tick) getOrElse Long.MinValue
      @volatile
      private[this] var state: Option[InUse] = wSnapshot.map(_.state)
      def next(tx: Transaction): Unit = {
        revision = tx.revision
        tick = tx.tick
        state = projector(tx, state) match {
          case null if state.isDefined =>
            throw new IllegalStateException(s"Projector returned `null`, replacing existing state: ${state.get}")
          case newState =>
            Option(newState)
        }
      }
      def result(): Future[Option[InUseSnapshot]] = Future.successful {
        state.map {
          new InUseSnapshot(_, revision, tick)
        }
      }
    }

    val fold = new SnapshotFold(snapshot.map(_.map(stateAugmentCodec(id).decode)))
    val newSnapshot: Future[Option[InUseSnapshot]] =
      snapshot match {
        case Some(snapshot) =>
          eventSource.replayStreamFrom(id, snapshot.revision + 1)(fold)
        case None =>
          eventSource.replayStream(id)(fold)
      }

    implicit def ec = Threads.PiggyBack
    newSnapshot.flatMap {
      case Some(newSnapshot) =>
        stateAugmentCodec(id).encode(newSnapshot.state)
          .map(ss => Some(newSnapshot.copy(state = ss)))
      case None =>
        Future.none
    }

  }

}

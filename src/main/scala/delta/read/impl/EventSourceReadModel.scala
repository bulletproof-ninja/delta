package delta.read.impl

import delta.process.AsyncCodec

import scuff.concurrent._

import scala.reflect.ClassTag
import scala.concurrent._

import delta._
import delta.read._


private[impl]
abstract class EventSourceReadModel[ID, ESID, EVT: ClassTag, Work >: Null, Stored] protected (
  protected val eventSource: EventSource[ESID, _ >: EVT])(
  implicit idConv: ID => ESID)
extends ReadModel[ID, Stored]
with StreamId[ID] {

  protected type StreamId = ESID
  protected def StreamId(id: ID) = idConv(id)
  protected type Transaction = delta.Transaction[_, _ >: EVT]
  protected type WSnapshot = delta.Snapshot[Work]

  protected def projector(tx: Transaction): Projector[Work, EVT]
  protected def stateCodec: AsyncCodec[Work, Stored]
  protected def stateCodecContext: ExecutionContext

  protected def replayToComplete(
      snapshot: Option[Snapshot], id: ESID)
      : Future[Option[Snapshot]] = {

    val projector = Projector(this.projector) _

    val replay = snapshot match {
      case Some(snapshot) =>
        eventSource.replayStreamFrom(id, snapshot.revision + 1) _
      case None =>
        eventSource.replayStream(id) _
    }

    val wSnapshot: Option[WSnapshot] = snapshot.map(_.map(stateCodec.decode))

    val newSnapshot: Future[Option[WSnapshot]] =
      StreamPromise.fold(wSnapshot, replay) {
        case (wSnapshot, tx) => {
          projector(tx, wSnapshot.map(_.content)) match {
            case null => None // Should not return null, but certain edge cases exist
            case newState => Some {
              new WSnapshot(newState, tx.revision, tx.tick)
            }
          }
        }
      }

      implicit def ec = stateCodecContext

    newSnapshot.flatMap {
      case Some(newSnapshot) =>
        stateCodec.encode(newSnapshot.content)
          .map(ss => Some(newSnapshot.copy(content = ss)))
      case None =>
        Future.none
    }

  }

}

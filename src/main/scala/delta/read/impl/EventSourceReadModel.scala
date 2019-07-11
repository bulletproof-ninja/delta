package delta.read.impl

import delta._
import scala.concurrent._
import scuff.concurrent.StreamPromise
import scala.reflect.ClassTag
import delta.read._

private[impl] abstract class EventSourceReadModel[ID, ESID, S >: Null: ClassTag, EVT: ClassTag] protected (
    protected val eventSource: EventSource[ESID, _ >: EVT],
    txProjector: TransactionProjector[S, EVT])
  extends BasicReadModel[ID, S] {

  protected type TXN = Transaction[ESID, _ >: EVT]

  protected def replayToComplete(snapshot: Option[Snapshot], id: ESID): Future[Option[Snapshot]] = {

    val replay = snapshot match {
      case Some(snapshot) =>
        eventSource.replayStreamFrom(id, snapshot.revision + 1) _
      case None =>
        eventSource.replayStream(id) _
    }

    StreamPromise.fold(snapshot, replay) {
      case (snapshot, txn) => {
        txProjector(txn, snapshot.map(_.content)) match {
          case null => None
          case newState => Some {
            new Snapshot(newState, txn.revision, txn.tick)
          }
        }

      }
    }

  }

}

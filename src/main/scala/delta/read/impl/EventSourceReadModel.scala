package delta.read.impl

import delta._
import scala.concurrent._
import scuff.concurrent.StreamPromise
import scala.reflect.ClassTag
import delta.read._

private[impl] abstract class EventSourceReadModel[ID, ESID, S >: Null: ClassTag, EVT: ClassTag] protected (
    protected val eventSource: EventSource[ESID, _ >: EVT],
    projectorSource: Either[Map[String, String] => Projector[S, EVT], Projector[S, EVT]])
  extends BasicReadModel[ID, S] {

  protected type TXN = Transaction[ESID, _ >: EVT]

  private[this] val projectorProcOrNull = projectorSource.right.toOption.map { projector =>
    Projector.process(projector) _
  }.orNull
  private[this] val getProjector = projectorSource.left.toOption.orNull

  protected def projectorProcess(metadata: Map[String, String]) =
    if (projectorProcOrNull != null) projectorProcOrNull
    else {
      assert(getProjector != null)
      val projector = getProjector(metadata)
      Projector.process(projector) _
    }

  protected def replayToComplete(snapshot: Option[Snapshot], id: ESID): Future[Option[Snapshot]] = {

    val replay = snapshot match {
      case Some(snapshot) =>
        eventSource.replayStreamFrom(id, snapshot.revision + 1) _
      case None =>
        eventSource.replayStream(id) _
    }

    StreamPromise.fold(snapshot, replay) {
      case (snapshot, txn) => {
        val process = projectorProcess(txn.metadata)
        process(snapshot.map(_.content), txn.events) match {
          case null => None
          case newState => Some {
            new Snapshot(newState, txn.revision, txn.tick)
          }
        }

      }
    }

  }

}

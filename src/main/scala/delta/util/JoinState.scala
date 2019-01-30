package delta.util

import scuff.ScuffMap
import scala.concurrent.Future
import delta.Snapshot
import delta.Transaction
import scala.reflect.ClassTag

object JoinState {

  final class Processor[JS](processState: Option[JS] => JS, val revision: Int) {
    def this(processState: Option[JS] => JS) = this(processState, -1)
    @inline def apply(state: Option[JS]): JS = processState(state)
  }

}

/**
 * Build additional, or exclusively, collateral
 * state, similar to an outer join.
 * NOTE: Because the state is not built from its
 * own stream, there's no guarantee of ordering
 * other than the monotonic order of the stream
 * itself. Duplicate events *may* be observed and
 * causal ordering cannot be expected (in fact
 * is unlikely), thus processing should be
 * forgiving in that regard.
 */
trait JoinState[ID, EVT, JS] {
  proc: TransactionProcessor[ID, _ >: EVT, _ >: JS] =>

  protected type Processor = JoinState.Processor[JS]

  protected final def Processor(process: Option[JS] => JS): Processor = new Processor(process)
  protected final def Processor(process: Option[JS] => JS, revision: Int): Processor = new Processor(process, revision)

  /**
   *  Pre-process event by returning map of indirect ids, if any,
   *  and the associated processing function.
   *  @param streamId The stream id of event
   *  @param streamRevision If duplicate processing is not idempotent, this can be used
   *  @param tick If causal ordering is necessary, use this
   *  @param evt Event from stream
   *  @return Map of collateral id and state processor(s) derivable from event, if any.
   */
  protected def join(streamId: ID, streamRevision: Int, tick: Long, evt: EVT, metadata: Map[String, String]): Map[ID, Processor]

  protected def preprocess(txn: Transaction[ID, _ >: EVT])(
      implicit evtTag: ClassTag[EVT]): Map[ID, Processor] = {
    import scuff._
    txn.events.foldLeft(Map.empty[ID, Processor]) {
      case (fmap, evt: EVT) =>
        val mapping = join(txn.stream, txn.revision, txn.tick, evt, txn.metadata)
        if (mapping.contains(txn.stream)) throw new IllegalStateException(
          s"JoinState preprocessing cannot contain stream id itself: ${txn.stream}")
        fmap.merge(mapping) {
          case (first, second) => Processor(
            option => {
              val js1 = first(option)
              val js2 = second(Some(js1))
              js2
            }, first.revision max second.revision)
        }
    }
  }
}

trait MonotonicJoinState[ID, EVT, S >: Null, JS >: Null <: S]
  extends MonotonicProcessor[ID, EVT, S]
  with JoinState[ID, EVT, JS] {

  protected def evtTag: ClassTag[EVT]

  protected final override def processAsync(txn: TXN, currState: Option[S]): Future[S] = {
    val processors: Map[ID, Processor] = this.preprocess(txn)(evtTag)
    val futureUpdates: Iterable[Future[Unit]] =
      processors.map {
        case (id, processor) =>
          implicit val ec = processingContext(id)
          val res: Future[(Option[SnapshotUpdate], _)] = this.processStore.upsert(id) { snapshot =>
            val tick = snapshot.map(_.tick max txn.tick) getOrElse txn.tick
            val updatedState = processor(snapshot.map(_.content.asInstanceOf[JS]))
            val updatedSnapshot = Snapshot(updatedState, processor.revision, tick)
            Future successful (Some(updatedSnapshot) -> null)
          }
          res.map {
            case (Some(update), _) => onSnapshotUpdate(id, update)
            // We only process `Some`, so we know there are no `None`
            case _ => sys.error("Should not happen.")
          }
      }
    implicit val ec = processingContext(txn.stream)
    Future.sequence(futureUpdates).flatMap { _ =>
      super.processAsync(txn, currState)
    }
  }
}

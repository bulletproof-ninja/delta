package delta.process

import scala.concurrent.Future
import delta.Snapshot
import scala.reflect.ClassTag

object JoinState {

  final class Processor[S](processState: Option[S] => S, val revision: Int) {
    def this(processState: Option[S] => S) = this(processState, -1)

    @inline def apply(state: Option[S]): S = processState(state)
    @inline def apply(state: S): S = apply(Option(state))

  }

  def Processor[S: ClassTag](process: Option[S] => S): Processor[S] = new Processor(process)
  def Processor[S: ClassTag](process: Option[S] => S, revision: Int): Processor[S] =
    new Processor(process, revision)

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
trait JoinState[ID, EVT, S >: Null] {
  proc: TransactionProcessor[ID, EVT, S] =>

  type Processor = JoinState.Processor[S]

  protected final def Processor(process: Option[S] => S): Processor =
    new Processor(process)
  protected final def Processor(process: Option[S] => S, revision: Int): Processor =
    new Processor(process, revision)

  /**
   *  Pre-process event by returning map of indirect ids, if any,
   *  and the associated processing function.
   *  @param streamId The stream id of event
   *  @param streamRevision If duplicate processing is not idempotent, this can be used
   *  @param tick If causal ordering is necessary, use this transaction tick
   *  @param metadata Transaction metadata
   *  @param streamState Current stream state (prior to applying transaction events)
   *  @param evt Event from stream
   *  @return Map of collateral id and state processor(s) derivable from event, if any.
   */
  protected def join(
      streamId: ID, streamRevision: Int,
      tick: Long, metadata: Map[String, String],
      streamState: Option[S])(evt: EVT): Map[ID, _ <: Processor]

  protected def join(txn: TXN, streamState: Option[S]): Map[ID, Processor] = {
    import scuff._
    val join = this.join(txn.stream, txn.revision, txn.tick, txn.metadata, streamState) _
    txn.events.foldLeft(Map.empty[ID, Processor]) {
      case (fmap, evt: EVT) =>
        val mapping = join(evt)
        if (mapping.contains(txn.stream)) throw new IllegalStateException(
          s"JoinState preprocessing cannot contain stream id itself: ${txn.stream}")
        fmap.merge(mapping) {
          case (first, second) => new JoinState.Processor(
            (state: Option[S]) => {
              val js1 = first(state)
              val js2 = second(js1)
              js2
            }, first.revision max second.revision)
        }
    }
  }
}

trait MonotonicJoinState[ID, EVT, S >: Null]
  extends MonotonicProcessor[ID, EVT, S]
  with JoinState[ID, EVT, S] {

  protected final override def processAsync(txn: TXN, streamState: Option[S]): Future[S] = {
    val processors: Map[ID, Processor] = this.join(txn, streamState)
    val futureUpdates: Iterable[Future[Unit]] =
      processors.map {
        case (id, processor) =>
          implicit val ec = processContext(id)
          val res: Future[(Option[SnapshotUpdate], _)] = this.processStore.upsert(id) { optSnapshot =>
            val tick = optSnapshot.map(_.tick max txn.tick) getOrElse txn.tick
            val updatedState: S = processor(optSnapshot.map(_.content))
            val updatedSnapshot = Snapshot(updatedState, processor.revision, tick)
            Future successful (Some(updatedSnapshot) -> null)
          }
          res.map {
            case (Some(update), _) => onSnapshotUpdate(id, update)
            // We only process `Some`, so we know there are no `None`
            case _ => sys.error("Should not happen.")
          }
      }
    implicit val ec = processContext(txn.stream)
    Future.sequence(futureUpdates).flatMap { _ =>
      super.processAsync(txn, streamState)
    }
  }
}

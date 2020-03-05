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

  /** Process stream as normally implemented through `process`. */
  protected def processStream(tx: Transaction, streamState: Option[S]): Future[S]

  protected def process(tx: Transaction, streamState: Option[S]): Future[S] =
    processStream(tx, streamState)


  /**
   *  Pre-process event by returning map of indirect ids, if any,
   *  and the associated processing function.
   *  @param streamId The stream id of event
   *  @param streamRevision If duplicate processing is not idempotent, this can be used
   *  @param tick If causal ordering is necessary, use this transaction tick
   *  @param metadata Transaction metadata
   *  @param evt Event from stream
   *  @return Map of joined stream ids and state processor(s) derivable from event
   */
  protected def prepareJoin(
      streamId: ID, streamRevision: Int,
      tick: Long, metadata: Map[String, String])(evt: EVT): Map[ID, _ <: Processor]

  protected def prepareJoin(tx: Transaction): Map[ID, Processor] = {
    import scuff._
    val join = prepareJoin(tx.stream, tx.revision, tx.tick, tx.metadata) _
    tx.events.foldLeft(Map.empty[ID, Processor]) {
      case (fmap, evt: EVT) =>
        val mapping = join(evt)
        if (mapping.contains(tx.stream)) throw new IllegalStateException(
          s"JoinState preprocessing cannot contain stream id itself: ${tx.stream}")
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

trait MonotonicJoinState[ID, EVT, S >: Null, U]
  extends MonotonicProcessor[ID, EVT, S, U]
  with JoinState[ID, EVT, S] {

  protected final override def process(tx: Transaction, streamState: Option[S]): Future[S] = {
    val processors: Map[ID, Processor] = prepareJoin(tx)
    val futureUpdates: Iterable[Future[Unit]] =
      processors.map {
        case (id, processor) =>
          implicit val ec = processContext(id)
          val result: Future[(Option[Update], _)] =
            this.processStore.upsert(id) { snapshot =>
              val tick = snapshot.map(_.tick max tx.tick) getOrElse tx.tick
              val updatedSnapshot = processor(snapshot.map(_.content)) match {
                case null => None
                case updatedState => Some {
                  Snapshot(updatedState, processor.revision, tick)
                }
              }
              Future successful updatedSnapshot -> (())
            }
          result.map {
            case (Some(update), _) => onUpdate(id, update)
            case _ => // No change
          }
      }
    implicit val ec = processContext(tx.stream)
    Future.sequence(futureUpdates).flatMap { _ =>
      super.process(tx, streamState)
    }
  }
}

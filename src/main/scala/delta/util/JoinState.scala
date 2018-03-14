package delta.util

import scuff.ScuffMap
import scala.concurrent.Future
import delta.Snapshot
import scuff.concurrent.Threads

/**
  * Build additional, or exclusively, collateral
  * state, similar to an outer join.
  * NOTE: Because the state is not built from its
  * own stream, there's no guarantee of ordering
  * other than the monotonic order of the stream
  * itself. Duplicate events may be observed and
  * causal ordering cannot be expected (in fact
  * is unlikely), thus processing should be
  * forgiving in that regard.
  */
trait JoinState[ID, EVT, S >: Null]
  extends MonotonicProcessor[ID, EVT, S] {

  protected type JoinState <: S

  protected final def Processor(process: Option[JoinState] => JoinState, revision: Int = -1): Processor = new Processor(process, revision)
  protected final class Processor(processState: Option[JoinState] => JoinState, val revision: Int) {
    @inline def apply(state: Option[JoinState]) = processState(state)
  }

  protected final override def processAsync(txn: TXN, currState: Option[S]): Future[S] = {
    val futureUpdates: Map[ID, Future[Update]] =
      preprocess(txn).map {
        case (id, processor) =>
          id -> this.processStore.upsert(id) { snapshot =>
            val tick = snapshot.map(_.tick max txn.tick) getOrElse txn.tick
            val updated = processor(snapshot.map(_.content.asInstanceOf[JoinState]))
            Future successful Some(Snapshot(updated, processor.revision, tick)) -> Unit
          }(executionContext(id)).map(_._1.get)(Threads.PiggyBack)
      }
    futureUpdates.foreach {
      case (id, futureUpdate) =>
        futureUpdate.foreach(onUpdate(id, _))(executionContext(id))
    }
    implicit val ec = executionContext(txn.stream)
    Future.sequence(futureUpdates.values).flatMap { _ =>
      super.processAsync(txn, currState)
    }
  }

  /**
    *  Pre-process event by returning map of indirect ids, if any,
    *  and the associated processing function.
    *  @param streamId The stream id of event
    *  @param streamRevision If duplicate processing is not idempotent, this can be used
    *  @param tick If causal ordering is necessary, use this
    *  @param evt Event from stream
    *  @return Map of collateral id and state processor(s) derivable from event, if any.
    */
  protected def preprocess(streamId: ID, streamRevision: Int, tick: Long, evt: EVT): Map[ID, Processor]

  private def preprocess(txn: TXN): Map[ID, Processor] = {
    import scuff._
    txn.events.foldLeft(Map.empty[ID, Processor]) {
      case (fmap, evt: EVT) =>
        val mapping = preprocess(txn.stream, txn.revision, txn.tick, evt)
        if (mapping.contains(txn.stream)) throw new IllegalStateException(
          s"Indirect preprocessing must not contain stream id itself: ${txn.stream}")
        fmap.merge(mapping) {
          case (first, second) => Processor(
            option => second(Some(first(option))),
            first.revision max second.revision)
        }
    }
  }
}

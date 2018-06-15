package delta.util

import scuff.ScuffMap
import scala.concurrent.Future
import delta.Snapshot
import scuff.concurrent.Threads
import delta.Transaction
import scala.reflect.ClassTag

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
trait JoinStateProcessor[ID, EVT, S >: Null, JS >: Null <: S]
  extends MonotonicProcessor[ID, EVT, S]
  with JoinState[ID, EVT, JS] {

  protected final override def processAsync(txn: TXN, currState: Option[S]): Future[S] = {
    val futureUpdates: Map[ID, Future[Update]] =
      preprocess(txn).map {
        case (id, processor) =>
          id -> this.processStore.upsert(id) { snapshot =>
            val tick = snapshot.map(_.tick max txn.tick) getOrElse txn.tick
            val updated = processor(snapshot.map(_.content.asInstanceOf[JS]))
            Future successful Some(Snapshot(updated, processor.revision, tick)) -> Unit
          }(processingContext(id)).map(_._1.get)(Threads.PiggyBack)
      }
    futureUpdates.foreach {
      case (id, futureUpdate) =>
        futureUpdate.foreach(onUpdate(id, _))(processingContext(id))
    }
    implicit val ec = processingContext(txn.stream)
    Future.sequence(futureUpdates.values).flatMap { _ =>
      super.processAsync(txn, currState)
    }
  }
}

object JoinState {
  final class Processor[JS](processState: Option[JS] => JS, val revision: Int) {
    def this(processState: Option[JS] => JS) = this(processState, -1)
    @inline def apply(state: Option[JS]) = processState(state)
  }

}
trait JoinState[ID, EVT, JS] {

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
  protected def preprocess(streamId: ID, streamRevision: Int, tick: Long, evt: EVT, metadata: Map[String, String]): Map[ID, Processor]

  def preprocess(txn: Transaction[ID, _ >: EVT])(implicit ev: ClassTag[EVT]): Map[ID, Processor] = {
    import scuff._
    txn.events.foldLeft(Map.empty[ID, Processor]) {
      case (fmap, evt: EVT) =>
        val mapping = preprocess(txn.stream, txn.revision, txn.tick, evt, txn.metadata)
        if (mapping.contains(txn.stream)) throw new IllegalStateException(
          s"JoinState preprocessing cannot contain stream id itself: ${txn.stream}")
        fmap.merge(mapping) {
          case (first, second) => Processor(
            option => second(Some(first(option))),
            first.revision max second.revision)
        }
    }
  }
}

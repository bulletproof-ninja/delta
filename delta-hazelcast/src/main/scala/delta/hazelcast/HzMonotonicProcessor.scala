package delta.hazelcast

import com.hazelcast.core.IMap

import scala.concurrent._
import scala.reflect.ClassTag
import scala.util._

import delta._
import delta.process._

class HzMonotonicProcessor[ID, EVT: ClassTag, S >: Null: ClassTag](
  es: EventSource[ID, _ >: EVT],
  imap: IMap[ID, _ <: EntryState[S, EVT]],
  getProjector: delta.Transaction[ID, _ >: EVT] => Projector[S, EVT],
  reportFailure: Throwable => Unit,
  config: LiveProcessConfig)(
  implicit
  ec: ExecutionContext)
extends (delta.Transaction[ID, _ >: EVT] => Future[EntryUpdateResult])
with MissingRevisionsReplay[ID, EVT] {

  private[this] val replay =
    replayMissingRevisions(es, Some(config.replayMissingDelay -> config.replayMissingScheduler)) _

  type Transaction = delta.Transaction[ID, _ >: EVT]
  def apply(tx: Transaction) = {
    val projector = getProjector(tx)
    DistributedMonotonicProcessor(imap, projector)(tx)
      .andThen {
        case Success(MissingRevisions(range)) =>
          replay(tx.stream, range)(apply)
            .failed
            .foreach(reportFailure)
        case Failure(th) =>
          reportFailure(th)
      }
  }
}

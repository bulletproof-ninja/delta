package delta.hazelcast

import com.hazelcast.core.IMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util._
import delta.process.MissingRevisionsReplay
import delta.EventSource
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.Future
import delta.Projector

class HzMonotonicProcessor[ID, EVT: ClassTag, S >: Null: ClassTag](
  es: EventSource[ID, _ >: EVT],
  imap: IMap[ID, _ <: EntryState[S, EVT]],
  getProjector: delta.Transaction[ID, _ >: EVT] => Projector[S, EVT],
  reportFailure: Throwable => Unit,
  missingRevisionsReplayDelay: FiniteDuration)(
  implicit
  ec: ExecutionContext,
  scheduler: ScheduledExecutorService)
extends (delta.Transaction[ID, _ >: EVT] => Future[EntryUpdateResult])
with MissingRevisionsReplay[ID, EVT] {

  private[this] val replay =
    replayMissingRevisions(es, missingRevisionsReplayDelay, scheduler, reportFailure) _

  type Transaction = delta.Transaction[ID, _ >: EVT]
  def apply(tx: Transaction) = {
    val projector = getProjector(tx)
    DistributedMonotonicProcessor(imap, projector)(tx)
      .andThen {
        case Success(MissingRevisions(range)) =>
          replay(tx.stream, range)(apply)
        case Failure(th) =>
          reportFailure(th)
      }
  }
}

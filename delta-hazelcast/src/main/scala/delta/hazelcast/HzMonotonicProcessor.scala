package delta.hazelcast

import delta.Transaction
import delta.TransactionProjector
import com.hazelcast.core.IMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util._
import delta.process.MissingRevisionsReplay
import delta.EventSource
import java.util.concurrent.ScheduledExecutorService

class HzMonotonicProcessor[ID, EVT: ClassTag, S >: Null: ClassTag](
    es: EventSource[ID, _ >: EVT],
    imap: IMap[ID, EntryState[S, EVT]],
    txnProjector: TransactionProjector[S, EVT],
    reportFailure: Throwable => Unit,
    missingRevisionsReplayScheduler: ScheduledExecutorService,
    missingRevisionsReplayDelay: FiniteDuration = 1111.milliseconds)
  extends (Transaction[ID, _ >: EVT] => Unit)
  with MissingRevisionsReplay[ID, EVT] {

  implicit private[this] val ec = 
    ExecutionContext.fromExecutorService(missingRevisionsReplayScheduler, reportFailure)

  private[this] val replay = 
    replayMissingRevisions(es, missingRevisionsReplayDelay, missingRevisionsReplayScheduler, reportFailure) _
  private[this] val process = 
    DistributedMonotonicProcessor(imap, txnProjector) _

  type TXN = Transaction[ID, _ >: EVT]
  def apply(txn: TXN) = {
    process(txn) andThen {
      case Success(MissingRevisions(range)) =>
        replay(txn.stream, range)(apply)
      case Failure(th) => reportFailure(th)
    }
  }
}

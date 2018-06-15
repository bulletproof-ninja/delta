package delta.hazelcast

import delta.Transaction
import delta.EventReducer
import com.hazelcast.core.IMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util._
import delta.util.MissingRevisionsReplay
import delta.EventSource
import java.util.concurrent.ScheduledExecutorService
import scuff.Codec

class HzMonotonicProcessor[ID, EVT: ClassTag, WS >: Null, RS >: Null](
    es: EventSource[ID, _ >: EVT],
    imap: IMap[ID, EntryState[RS, EVT]],
    stateCodec: Codec[RS, WS],
    reducer: EventReducer[WS, EVT],
    reportFailure: Throwable => Unit,
    missingRevisionsReplayScheduler: ScheduledExecutorService,
    missingRevisionsReplayDelay: FiniteDuration = 1111.milliseconds)
  extends (Transaction[ID, _ >: EVT] => Unit)
  with MissingRevisionsReplay[ID, EVT] {

  implicit private[this] val ec = ExecutionContext.fromExecutorService(missingRevisionsReplayScheduler, reportFailure)

  private[this] val replay = onMissingRevisions(es, missingRevisionsReplayScheduler, reportFailure) _
  private[this] val process = DistributedMonotonicProcessor(imap, stateCodec, reducer) _

  type TXN = Transaction[ID, _ >: EVT]
  def apply(txn: TXN) = {
    process(txn) andThen {
      case Success(MissingRevisions(range)) =>
        replay(txn.stream, range, missingRevisionsReplayDelay)(apply)
      case Failure(th) => reportFailure(th)
    }
  }
}

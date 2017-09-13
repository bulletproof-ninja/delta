package delta.hazelcast

import java.util.Map.Entry

import scala.concurrent.{ Future, Promise }

import com.hazelcast.core.{ ExecutionCallback, IMap }
import com.hazelcast.map.AbstractEntryProcessor

import delta.Snapshot

object EntryStateUpdater {
  /**
    * Update entry state.
    */
  def apply[K, D >: Null, EVT](imap: IMap[K, EntryState[D, EVT]])(key: K, snapshot: Snapshot[D]): Future[Unit] = {
    val updater = new EntryStateUpdater[K, D](snapshot)
    val promise = Promise[Unit]()
    val callback = new ExecutionCallback[Any] {
      def onResponse(response: Any) = promise.success(())
      def onFailure(th: Throwable) = promise failure th
    }
    imap.submitToKey(key, updater, callback)
    promise.future
  }
}

private final class EntryStateUpdater[K, D] private[hazelcast] (val snapshot: Snapshot[D])
    extends AbstractEntryProcessor[K, EntryState[D, Any]](true) {

  type S = EntryState[D, Any]

  def process(entry: Entry[K, S]): Object = {
    entry.getValue match {
      case null =>
        entry setValue new EntryState(snapshot, contentUpdated = true)

      case EntryState(null, _, unapplied) =>
        val remainingUnapplied = unapplied.dropWhile(_._1 <= snapshot.revision)
        entry setValue new EntryState(snapshot, contentUpdated = remainingUnapplied.isEmpty, remainingUnapplied)

      case EntryState(Snapshot(_, revision, _), _, unapplied) if snapshot.revision > revision =>
        val remainingUnapplied = unapplied.dropWhile(_._1 <= snapshot.revision)
        entry setValue new EntryState(snapshot, contentUpdated = remainingUnapplied.isEmpty, remainingUnapplied)

      case _ => // Ignore
    }
    null
  }

}

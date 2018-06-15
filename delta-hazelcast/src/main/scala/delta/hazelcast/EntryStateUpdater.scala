package delta.hazelcast

import java.util.Map.Entry

import scala.concurrent.Future

import com.hazelcast.core.IMap
import com.hazelcast.map.AbstractEntryProcessor

import delta.Snapshot

object EntryStateUpdater {
  /**
    * Update entry state.
    */
  def apply[K, EVT, S](imap: IMap[K, EntryState[S, EVT]])(key: K, snapshot: Snapshot[S]): Future[Unit] = {
    val updater = new EntryStateUpdater[K, EVT, S](snapshot)
    val callback = CallbackPromise[Any, Unit](_ => ())
    imap.submitToKey(key, updater, callback)
    callback.future
  }
}

private final class EntryStateUpdater[K, EVT, S] private[hazelcast] (val snapshot: Snapshot[S])
    extends AbstractEntryProcessor[K, EntryState[S, EVT]](true) {

  type EntryState = delta.hazelcast.EntryState[S, EVT]

  def process(entry: Entry[K, EntryState]): Object = {
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

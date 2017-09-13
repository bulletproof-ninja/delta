package delta.hazelcast

import java.util.Map.Entry

import com.hazelcast.map.AbstractEntryProcessor

import delta.{ Snapshot, SnapshotStore }

/**
 * Internal class used by [[delta.hazelcast.IMapSnapshotStore]].
 */
class SnapshotUpdater[K, D] private[hazelcast] (
  val update: Either[(Int, Long), Snapshot[D]])
    extends AbstractEntryProcessor[K, Snapshot[D]](true) {

  def process(entry: Entry[K, Snapshot[D]]): Object = {
    entry.getValue match {
      case null => update match {
        case Right(snapshot) => entry setValue snapshot
        case Left(_) => throw SnapshotStore.Exceptions.refreshNonExistent(entry.getKey)
      }
      case existing => update match {
        case Right(snapshot) =>
          if (snapshot.revision >= existing.revision && snapshot.tick >= existing.tick) {
            entry setValue snapshot
          } else {
            throw SnapshotStore.Exceptions.writeOlderRevision(entry.getKey, existing, snapshot)
          }
        case Left((revision, tick)) =>
          val updated = {
            val updated = if (revision > existing.revision) existing.copy(revision = revision) else existing
            if (tick > existing.tick) updated.copy(tick = tick)
            else updated
          }
          if (updated ne existing) {
            entry setValue updated
          }
      }
    }
    null
  }

}

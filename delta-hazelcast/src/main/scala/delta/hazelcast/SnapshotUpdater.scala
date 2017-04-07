package delta.hazelcast

import delta.Snapshot
import com.hazelcast.map.AbstractEntryProcessor
import java.util.Map.Entry

class SnapshotUpdater[K, D] private (
  val update: Either[(Int, Long), Snapshot[D]])
    extends AbstractEntryProcessor[K, Snapshot[D]](true) {

  def this(snapshot: Snapshot[D]) = this(Right(snapshot))
  def this(revision: Int, tick: Long) = this(Left(revision -> tick))

  def process(entry: Entry[K, Snapshot[D]]): Object = {
    val toUpdate =
      entry.getValue match {
        case null => update.right.toOption
        case existing => update match {
          case Right(snapshot) if snapshot.revision >= existing.revision =>
            Some(snapshot)
          case Left((revision, tick)) if revision > existing.revision || tick > existing.tick =>
            Some(existing.copy(revision = revision, tick = tick))
          case _ => None
        }
      }
    toUpdate.foreach(entry.setValue)
    null
  }

}

package delta.hazelcast

import com.hazelcast.map.EntryProcessor
import java.util.Map.Entry
import com.hazelcast.core.ReadOnly

object EntryStateSnapshotReader
extends EntryStateSnapshotReader[Object, Object](null)

class EntryStateSnapshotReader[ES, S](val stateConv: ES => S)
extends EntryProcessor[Object, EntryState[ES, _]]
with ReadOnly {

  def process(entry: Entry[Object, EntryState[ES, _]]): Object = {
    if (entry.getValue == null) null
    else entry.getValue.snapshot match {
      case null => null
      case snapshot if stateConv != null => snapshot map stateConv
      case snapshot => snapshot
    }
  }

  def getBackupProcessor = null

}

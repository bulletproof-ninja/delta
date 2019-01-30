package delta.hazelcast

import com.hazelcast.map.EntryProcessor
import java.util.Map.Entry
import com.hazelcast.core.ReadOnly

object EntryStateSnapshotReader
  extends EntryProcessor[Object, EntryState[_, _]]
  with ReadOnly {
  def process(entry: Entry[Object, EntryState[_, _]]): Object = {
    if (entry.getValue == null) null
    else entry.getValue.snapshot
  }
  def getBackupProcessor = null
}

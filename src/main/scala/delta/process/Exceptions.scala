package delta.process

import delta.Snapshot

private[delta] object Exceptions {
  def refreshNonExistent(key: Any) = new IllegalStateException(
    s"Tried to refresh non-existent snapshot $key")
  def writeOlder(key: Any, existing: Snapshot[_], update: Snapshot[_]): IllegalStateException =
    writeOlder(key, existing.revision, update.revision, existing.tick, update.tick)
  def writeOlder(
      key: Any,
      existingRev: Int, updateRev: Int,
      existingTick: Long, updateTick: Long): IllegalStateException = {
    val msg =
      if (updateRev < existingRev) {
        s"Tried to update snapshot $key (rev:$existingRev, tick:$existingTick) with older revision $updateRev"
      } else if (updateTick < existingTick) {
        s"Tried to update snapshot $key (rev:$existingRev, tick:$existingTick) with older tick $updateTick"
      } else {
        s"Failed to update snapshot $key (rev:$existingRev, tick:$existingTick) for unknown reason"
      }
    new IllegalStateException(msg)
  }

}

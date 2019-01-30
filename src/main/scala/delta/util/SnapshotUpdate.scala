package delta.util

import delta.Snapshot

case class SnapshotUpdate[+Content](snapshot: Snapshot[Content], contentUpdated: Boolean) {
  def map[C](m: Content => C): SnapshotUpdate[C] = {
    new SnapshotUpdate[C](snapshot.map(m), contentUpdated)
  }
}

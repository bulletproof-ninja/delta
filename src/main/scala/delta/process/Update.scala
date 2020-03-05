package delta.process

import delta.Snapshot

case class Update[+Content](changed: Option[Content], revision: Int, tick: Long) {

  def this(snapshot: Snapshot[Content], contentUpdated: Boolean) =
    this(if (contentUpdated) Some(snapshot.content) else None, snapshot.revision, snapshot.tick)

  def map[C](m: Content => C): Update[C] =
    if (changed.isEmpty) this.asInstanceOf[Update[C]]
    else this.copy(changed = changed.map(m(_)))

  def toSnapshot: Option[Snapshot[Content]] = changed.map(Snapshot(_, revision, tick))

}

object Update {
  def apply[Content](snapshot: Snapshot[Content], contentUpdated: Boolean) =
    new Update(snapshot, contentUpdated)
}

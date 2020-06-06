package delta.process

import delta._

final case class Update[+S](changed: Option[S], revision: Revision, tick: Tick) {

  def this(snapshot: Snapshot[S], stateUpdated: Boolean) =
    this(if (stateUpdated) Some(snapshot.state) else None, snapshot.revision, snapshot.tick)

  def map[C](m: S => C): Update[C] =
    if (changed.isEmpty) this.asInstanceOf[Update[C]]
    else this.copy(changed = changed.map(m(_)))

  def flatMap[C](fm: S => Option[C]): Update[C] =
    if (changed.isEmpty) this.asInstanceOf[Update[C]]
    else this.copy(changed = changed.flatMap(fm(_)))

  def toSnapshot: Option[Snapshot[S]] = changed.map(Snapshot(_, revision, tick))

}

object Update {
  def apply[S](snapshot: Snapshot[S], stateUpdated: Boolean) =
    new Update(snapshot, stateUpdated)
}

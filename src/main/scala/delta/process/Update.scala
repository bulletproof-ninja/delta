package delta.process

import delta._

/**
  * `Update` represent a partial, or full, representation of
  *  a `Snapshot` update.
  * @param change State change, if any
  * @param revision Updated revision
  * @param tick Updated tick
  */
final case class Update[+S](change: Option[S], revision: Revision, tick: Tick) {

  def this(snapshot: Snapshot[S], stateUpdated: Boolean) =
    this(if (stateUpdated) Some(snapshot.state) else None, snapshot.revision, snapshot.tick)

  def map[C](mod: S => C): Update[C] =
    if (change.isEmpty) this.asInstanceOf[Update[C]]
    else this.copy(change = change map mod)

  def flatMap[C](mod: S => Option[C]): Update[C] =
    if (change.isEmpty) this.asInstanceOf[Update[C]]
    else this.copy(change = change flatMap mod)

  /** If update has state change, map to snapshot. */
  def toSnapshot: Option[Snapshot[S]] = change.map(Snapshot(_, revision, tick))

}

object Update {
  def apply[S](snapshot: Snapshot[S], stateUpdated: Boolean): Update[S] =
    new Update(snapshot, stateUpdated)
}

package delta.process

/**
  * Translate Snapshots to Updates and vice versa.
  */
abstract class UpdateCodec[S, U] {

  type Snapshot = delta.Snapshot[S]
  type Update = delta.process.Update[U]

  /**
    * Derive update from new state (and optionally old state).
    * @note This should always be possible, as the update can simply be the current state itself
    * @param prevState Previous state, if exists
    * @param currState Current state
    * @return The difference between `prevState` and `currState`, or simply just `currState` if updates are complete
    */
  def asUpdate(prevState: Option[S], currState: S): U

  /**
    * Apply update to old state to produce new state,
    * @note This may not always be possible, in which
    * case `None` should be returned.
    * @param state The state to update. `None` means no previous state exists
    * @param update The update to apply
    * @return new updated state, or `None` if not possible
    */
  def updateState(state: Option[S], update: U): Option[S]

  final def asUpdate(
      prevSnapshot: Option[Snapshot], currSnapshot: Snapshot, contentUpdated: Boolean)
      : Update = {

    val change = if (contentUpdated) Option {
      asUpdate(prevSnapshot.map(_.state), currSnapshot.state)
    } else None

    new Update(change, currSnapshot.revision, currSnapshot.tick)

  }

  final def updateState(
      prevSnapshot: Option[Snapshot], update: Update)
      : Option[Snapshot] = {

    update.change match {
      case None =>
        prevSnapshot.map {
          _.copy(revision = update.revision, tick = update.tick)
        }
      case Some(updateContent) =>
        updateState(prevSnapshot.map(_.state), updateContent)
          .map(new Snapshot(_, update.revision, update.tick))
    }

  }

}

object UpdateCodec {
  private[this] val none = new UpdateCodec[Any, Null] {
    def asUpdate(prevState: Option[Any], currState: Any): Null = null
    def updateState(prevState: Option[Any], update: Null): Option[Any] = scala.None
  }
  private[process] def None[S, U] = none.asInstanceOf[UpdateCodec[S, U]]

}

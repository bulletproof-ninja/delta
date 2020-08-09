package delta

import scala.util.control.NoStackTrace
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

package read {

  /** General read failure. */
  sealed trait ReadRequestFailure extends Exception with NoStackTrace {
    def id: Any
  }

  /** The provided revision is unknown, i.e. hasn't occurred yet. */
  sealed trait UnknownRevisionRequested extends ReadRequestFailure {
    def knownRevision: Revision
  }

  /** The provided id is unknown. */
  sealed trait UnknownIdRequested extends ReadRequestFailure

  /** The provided tick is unknown for given id, i.e. hasn't occurred yet. */
  sealed trait UnknownTickRequested extends ReadRequestFailure {
    def knownTick: Tick
  }

  /**
   * The read timed-out.
   * If we cannot definitively say that a read
   * failed due to unknown id, tick, or invalid revision,
   * we time out.
   */
  class Timeout private[read] (val id: Any, val timeout: FiniteDuration, msg: String)
    extends java.util.concurrent.TimeoutException(msg)
    with ReadRequestFailure

}

/**
  * Support for read models.
  */
package object read {

  private[delta] val DefaultReadTimeout = 5555.millis

  private[read] def UnknownIdRequested[ID](snapshotId: ID) =
    new IllegalArgumentException(s"Unknown id: $snapshotId") with UnknownIdRequested { def id = snapshotId }

  private[read] def Timeout[ID](
      id: ID, snapshot: Option[Snapshot[_]],
      minRevision: Revision, minTick: Tick, timeout: FiniteDuration): ReadRequestFailure = {
    snapshot match {
      case None =>
        val errMsg = s"Failed to find `$id`, within timeout of $timeout"
        new Timeout(id, timeout, errMsg) with UnknownIdRequested
      case Some(snapshot) =>
        assert(minRevision != -1 || minTick != Long.MinValue)
        val revText = if (minRevision != -1) s"revision >= $minRevision" else ""
        val tickText = if (minTick != Long.MinValue) s"tick >= $minTick" else ""
        val failedCondition =
          if (revText == "") tickText
          else if (tickText == "") revText
          else s"BOTH $revText AND $tickText"
        val errMsg = s"Failed to find $id with $failedCondition, within timeout of $timeout"
        if (minRevision != -1)
          if (minTick != Long.MinValue)
            new Timeout(id, timeout, errMsg) with UnknownRevisionRequested with UnknownTickRequested {
              def knownRevision = snapshot.revision; def knownTick = snapshot.tick
            }
          else
            new Timeout(id, timeout, errMsg) with UnknownRevisionRequested { def knownRevision = snapshot.revision }
        else
          new Timeout(id, timeout, errMsg) with UnknownTickRequested { def knownTick = snapshot.tick }
    }
  }

  private[delta] def verify[ID, S](
    id: ID, snapshot: Option[Snapshot[S]]): Snapshot[S] =
    snapshot match {
      case None =>
        throw UnknownIdRequested(id)
      case Some(snapshot) => snapshot
    }

  private[delta] def verifySnapshot[ID, S](
      id: ID, optSnapshot: Option[Snapshot[S]],
      minRevision: Revision): Snapshot[S] =
    verifySnapshot(id, verify(id, optSnapshot), minRevision, Long.MinValue)

  private[delta] def verifySnapshot[ID, S](
      id: ID, optSnapshot: Option[Snapshot[S]],
      minTick: Tick): Snapshot[S] =
    verifySnapshot(id, verify(id, optSnapshot), -1, minTick)

  private[delta] def verifySnapshot[ID, S](
      snapshotId: ID, snapshot: Snapshot[S],
      minRev: Revision, minTick: Tick): snapshot.type = {
    verifyRevision(snapshotId, snapshot, minRev)
    verifyTick(snapshotId, snapshot, minTick)
  }
  private[delta] def verifySnapshot[ID, S](
      snapshotId: ID, optSnapshot: Option[Snapshot[S]],
      minRev: Revision, minTick: Tick): Snapshot[S] = {
    val snapshot = verify(snapshotId, optSnapshot)
    verifyRevision(snapshotId, snapshot, minRev)
    verifyTick(snapshotId, snapshot, minTick)
  }

  private[delta] def verifyRevision[ID, S](
      snapshotId: ID, snapshot: Snapshot[S], revision: Revision): snapshot.type = {
    if (snapshot.revision < revision)
      throw new IllegalArgumentException(
        s"Unknown revision: $revision") with UnknownRevisionRequested {
          def id = snapshotId; def knownRevision = snapshot.revision
        }
    snapshot
  }

  private[delta] def verifyTick[ID, S](
      snapshotId: ID, snapshot: Snapshot[S], tick: Tick): snapshot.type = {
    if (snapshot.tick < tick)
      throw new IllegalArgumentException(
        s"Unknown tick: $tick") with UnknownTickRequested {
          def id = snapshotId; def knownTick = snapshot.tick
        }
    snapshot
  }

  private[delta] def verify[ID, S](
      snapshotId: ID, snapshot: Option[Snapshot[S]],
      minRev: Revision, minTick: Tick): Future[Snapshot[S]] =
    try Future successful verifySnapshot(snapshotId, snapshot, minRev, minTick) catch {
      case NonFatal(cause) => Future failed cause
    }

}

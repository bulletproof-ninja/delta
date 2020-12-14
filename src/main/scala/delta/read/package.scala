package delta

import scala.util.control.NoStackTrace
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

package read {

  /** General read failure. */
  trait ReadRequestFailure extends Exception with NoStackTrace {
    def id: Any
  }

  /** The provided revision is unknown, i.e. hasn't occurred yet. */
  trait UnknownRevisionRequested extends ReadRequestFailure {
    def knownRevision: Revision
  }

  /** The provided id is unknown. */
  trait UnknownIdRequested extends ReadRequestFailure

  /** The provided tick is unknown for given id, i.e. hasn't occurred yet. */
  trait UnknownTickRequested extends ReadRequestFailure {
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

  private[read] final case class UnknownId[ID](id: ID)
  extends IllegalArgumentException(s"Unknown id requested: $id")
  with UnknownIdRequested

}

/**
  * Support for read models.
  */
package object read {

  private[delta] val DefaultReadTimeout = 5555.millis

  private[delta] def verify[ID, S](
    id: ID, snapshot: Option[Snapshot[S]]): Snapshot[S] =
    snapshot match {
      case None =>
        throw UnknownId(id)
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

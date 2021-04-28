package delta

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

package read {

  /** General read failure. */
  trait ReadRequestFailure extends Exception {
    def id: Any
  }

  /** The provided revision is unknown, i.e. hasn't occurred yet. */
  trait UnknownRevisionRequested extends ReadRequestFailure {
    def requestedRevision: Revision
    def knownRevision: Revision
  }

  /** The provided id is unknown. */
  trait UnknownIdRequested extends ReadRequestFailure

  /** The provided tick is unknown for given id, i.e. hasn't occurred yet. */
  trait UnknownTickRequested extends ReadRequestFailure {
    def requestedTick: Tick
    def knownTick: Tick
  }

  /**
   * The read timed-out.
   * If we cannot definitively say that a read
   * failed due to unknown id, tick, or invalid revision,
   * we time out.
   */
  class Timeout private[read] (
    val id: Any, val timeout: FiniteDuration, msg: String)
  extends java.util.concurrent.TimeoutException(msg)
  with ReadRequestFailure

  private[delta] final class UnknownId[ID](val id: ID, name: String)
  extends IllegalArgumentException(s"Unknown `$name` requested: $id")
  with UnknownIdRequested

}

/**
  * Support for read models.
  */
package object read {

  private[delta] val DefaultReadTimeout = 5555.millis

  def verify[ID, S](
      id: ID, snapshot: Option[Snapshot[S]], name: String)
      : Snapshot[S] =
    snapshot match {
      case None =>
        throw new UnknownId(id, name)
      case Some(snapshot) => snapshot
    }

  private[delta] def verifySnapshot[ID, S](
      id: ID, optSnapshot: Option[Snapshot[S]],
      minRevision: Revision, name: String)
      : Snapshot[S] =
    verifySnapshot(id, verify(id, optSnapshot, name), minRevision, Long.MinValue, name)

  private[delta] def verifySnapshot[ID, S](
      id: ID, optSnapshot: Option[Snapshot[S]],
      minTick: Tick, name: String)
      : Snapshot[S] =
    verifySnapshot(id, verify(id, optSnapshot, name), -1, minTick, name)

  private[delta] def verifySnapshot[ID, S](
      snapshotId: ID, snapshot: Snapshot[S],
      minRev: Revision, minTick: Tick, name: String)
      : snapshot.type = {
    verifyRevision(snapshotId, snapshot, minRev, name)
    verifyTick(snapshotId, snapshot, minTick, name)
  }
  private[delta] def verifySnapshot[ID, S](
      snapshotId: ID, optSnapshot: Option[Snapshot[S]],
      minRev: Revision, minTick: Tick, name: String)
      : Snapshot[S] = {
    val snapshot = verify(snapshotId, optSnapshot, name)
    verifyRevision(snapshotId, snapshot, minRev, name)
    verifyTick(snapshotId, snapshot, minTick, name)
  }

  private[delta] def verifyRevision[ID, S](
      snapshotId: ID, snapshot: Snapshot[S], revision: Revision, name: String)
      : snapshot.type = {
    if (snapshot.revision < revision)
      throw new IllegalArgumentException(
        s"Unknown revision $revision for `$name` id: $snapshotId") with UnknownRevisionRequested {
          def id = snapshotId; def knownRevision = snapshot.revision; def requestedRevision = revision
        }
    snapshot
  }

  private[delta] def verifyTick[ID, S](
      snapshotId: ID, snapshot: Snapshot[S], tick: Tick, name: String)
      : snapshot.type = {
    if (snapshot.tick < tick)
      throw new IllegalArgumentException(
        s"Unknown tick $tick for `$name` id: $snapshotId") with UnknownTickRequested {
          def id = snapshotId; def knownTick = snapshot.tick; def requestedTick = tick
        }
    snapshot
  }

  private[delta] def verify[ID, S](
      snapshotId: ID, snapshot: Option[Snapshot[S]],
      minRev: Revision, minTick: Tick, name: String)
      : Future[Snapshot[S]] =
    try Future successful verifySnapshot(snapshotId, snapshot, minRev, minTick, name) catch {
      case NonFatal(cause) => Future failed cause
    }

}

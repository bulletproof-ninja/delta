package delta

import scala.util.control.NoStackTrace
import scala.concurrent.Future
import scala.concurrent.duration._

package read {

  /** General read failure. */
  sealed trait ReadRequestFailure extends Exception with NoStackTrace {
    def id: Any
  }

  /** The provided revision is unknown, i.e. hasn't occurred yet. */
  sealed trait UnknownRevisionRequested extends ReadRequestFailure {
    def knownRevision: Int
  }

  /** The provided id is unknown. */
  sealed trait UnknownIdRequested extends ReadRequestFailure

  /** The provided tick is unknown for given id, i.e. hasn't occurred yet. */
  sealed trait UnknownTickRequested extends ReadRequestFailure {
    def knownTick: Long
  }

  /**
   * The read timed-out.
   * If we cannot definitively say that a read
   * failed due to unknown id, tick, or invalid revision,
   * we time out.
   */
  class Timeout private[read] (val id: Any, val timeout: FiniteDuration, msg: String)
    extends java.util.concurrent.TimeoutException(msg) {
    withTrait: ReadRequestFailure =>
  }

}

package object read {

  private[delta] val DefaultReadTimeout = 4444.millis

  private[read] def UnknownIdRequested[ID](snapshotId: ID) =
    new IllegalArgumentException(s"Unknown id: $snapshotId") with UnknownIdRequested { def id = snapshotId }

  private[read] def Timeout[ID](
      id: ID, snapshot: Option[Snapshot[_]],
      minTickOrRevision: Either[Long, Int], timeout: FiniteDuration): ReadRequestFailure = {
    snapshot match {
      case None =>
        val errMsg = s"Failed to find $id, within timeout of $timeout"
        new Timeout(id, timeout, errMsg) with UnknownIdRequested
      case Some(snapshot) => minTickOrRevision match {
        case Left(minTick) =>
          val errMsg = s"Failed to find $id with tick >= $minTick, within timeout of $timeout"
          new Timeout(id, timeout, errMsg) with UnknownTickRequested { def knownTick = snapshot.tick }
        case Right(minRevision) =>
          val errMsg = s"Failed to find $id with revision >= $minRevision, within timeout of $timeout"
          new Timeout(id, timeout, errMsg) with UnknownRevisionRequested { def knownRevision = snapshot.revision }
      }
    }
  }

  private[delta] def verify[ID, S](
      snapshotId: ID, snapshot: Option[Snapshot[S]]): Future[Snapshot[S]] =
    snapshot match {
      case Some(snapshot) => Future successful snapshot
      case None => Future failed UnknownIdRequested(snapshotId)
    }

  private[delta] def verifyRevision[ID, S](
      snapshotId: ID, snapshot: Option[Snapshot[S]],
      minRevision: Int): Future[Snapshot[S]] =
    snapshot match {
      case Some(snapshot) => verifyRevision(snapshotId, snapshot, minRevision)
      case None => Future failed UnknownIdRequested(snapshotId)
    }
  private[delta] def verifyRevision[ID, S](
      snapshotId: ID, snapshot: Snapshot[S],
      minRevision: Int): Future[Snapshot[S]] =
    if (snapshot.revision >= minRevision) Future successful snapshot
    else Future failed
      new IllegalArgumentException(s"Unknown revision: $minRevision") with UnknownRevisionRequested { def id = snapshotId; def knownRevision = snapshot.revision }

  private[delta] def verifyTick[ID, S](
      snapshotId: ID, snapshot: Option[Snapshot[S]],
      minTick: Long): Future[Snapshot[S]] =
    snapshot match {
      case Some(snapshot) => verifyTick(snapshotId, snapshot, minTick)
      case None => Future failed UnknownIdRequested(snapshotId)
    }
  private[delta] def verifyTick[ID, S](
      snapshotId: ID, snapshot: Snapshot[S],
      minTick: Long): Future[Snapshot[S]] =
    if (snapshot.tick >= minTick) Future successful snapshot
    else Future failed
      new IllegalArgumentException(s"Unknown tick: $minTick") with UnknownTickRequested { def id = snapshotId; def knownTick = snapshot.tick }

}

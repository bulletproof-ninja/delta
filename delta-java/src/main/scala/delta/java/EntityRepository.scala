package delta.java

import java.util.Optional
import java.util.function.BiFunction

import delta.{ Revision, EventStore }
import delta.write.Metadata

import scala.concurrent._
import scala.util.control.NonFatal

import scuff.concurrent.Threads.PiggyBack

class EntityRepository[ESID, EVT, S >: Null, ID, ET](
  entity: Entity[ID, ET, S, EVT] { type Type = ET },
  eventStore: EventStore[ESID, _ >: EVT],
  exeCtx: ExecutionContext,
  idConv: Function1[ID, ESID]) {

  private[this] val repo = new delta.write.EntityRepository[ESID, EVT, S, ID, ET](entity)(eventStore, exeCtx)(idConv)

  private def toJInt(i: Int): Integer = Integer valueOf i
  private def toJInt(t: (ET, Int)): (ET, Integer) = (t._1, Integer valueOf t._2)

  def exists(id: ID): Future[Optional[Integer]] = repo.exists(id).map {
    case Some(rev) => Optional.of(rev: Integer)
    case None => Optional.empty[Integer]
  }(PiggyBack)

  def load(id: ID): Future[(ET, Integer)] = repo.load(id).map(toJInt)(PiggyBack)

  def insert(id: => ID, entity: ET): Future[ID] =
    repo.insert(id, entity)(Metadata.Empty)
  def insert(id: => ID, entity: ET, metadata: Metadata): Future[ID] =
    repo.insert(id, entity)(metadata)

  def update(
      id: ID, expectedRevision: Option[Revision],
      consumer: BiFunction[ET, Integer, Metadata])
      : Future[Integer] = {
    repo.update(id, expectedRevision) {
      case (entity, revision) =>
        try Future successful consumer.apply(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }.map(toJInt)(PiggyBack)
  }

  def update(id: ID, consumer: BiFunction[ET, Integer, Metadata]): Future[Integer] = {
    repo.update(id) {
      case (entity, revision) =>
        try Future successful consumer.apply(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }.map(toJInt)(PiggyBack)
  }

  def updateAsync(
      id: ID, expectedRevision: Option[Revision],
      withReturn: BiFunction[ET, Integer, Future[Metadata]]): Future[Integer] = {
    repo.update(id, expectedRevision) {
      case (entity, revision) =>
        try withReturn(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }.map(toJInt)(PiggyBack)
  }

  def updateAsync(id: ID, withReturn: BiFunction[ET, Integer, Future[Metadata]]): Future[Integer] = {
    repo.update(id) {
      case (entity, revision) =>
        try withReturn(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }.map(toJInt)(PiggyBack)
  }

}

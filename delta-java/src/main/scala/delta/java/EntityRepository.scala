package delta.java

import delta.EventStore
import scala.concurrent.ExecutionContext

import scala.concurrent.Future
import scuff.concurrent.Threads.PiggyBack

import java.util.function.BiConsumer
import scala.util.control.NonFatal
import java.util.function.BiFunction
import java.util.Optional
import delta.ddd.Metadata

class EntityRepository[ESID, EVT, S >: Null, ID, ET](
    entity: Entity[ID, ET, S, EVT] { type Type = ET },
    eventStore: EventStore[ESID, _ >: EVT],
    exeCtx: ExecutionContext,
    idConv: Function1[ID, ESID]) {

  private[this] val repo = new delta.ddd.EntityRepository[ESID, EVT, S, ID, ET](entity, exeCtx)(eventStore)(idConv)

  private def toJInt(t: (Any, Int)): Integer = Integer valueOf t._2
  private def toJInt(t: (ET, Int)): (ET, Integer) = (t._1, Integer valueOf t._2)

  def exists(id: ID): Future[Optional[Integer]] = repo.exists(id).map {
    case Some(rev) => Optional.of(rev: Integer)
    case None => Optional.empty[Integer]
  }(PiggyBack)

  def load(id: ID): Future[(ET, Integer)] = repo.load(id).map(toJInt)(PiggyBack)

  def insert(id: => ID, entity: ET): Future[ID] =
    repo.insert(id, entity)(Metadata.empty)
  def insert(id: => ID, entity: ET, metadata: Metadata): Future[ID] =
    repo.insert(id, entity)(metadata)

  def update(id: ID, expectedRevision: Option[Int], metadata: Metadata, consumer: BiConsumer[ET, Integer]): Future[Integer] = {
    repo.update(id, expectedRevision) {
      case (entity, revision) =>
        try Future successful consumer.accept(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }(metadata).map(toJInt)(PiggyBack)
  }
  def update(id: ID, expectedRevision: Option[Int], consumer: BiConsumer[ET, Integer]): Future[Integer] = {
    repo.update(id, expectedRevision) {
      case (entity, revision) =>
        try Future successful consumer.accept(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }(Metadata.empty).map(toJInt)(PiggyBack)
  }
  def update(id: ID, metadata: Metadata, consumer: BiConsumer[ET, Integer]): Future[Integer] = {
    repo.update(id) {
      case (entity, revision) =>
        try Future successful consumer.accept(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }(metadata).map(toJInt)(PiggyBack)
  }

  def updateReturn[R](id: ID, expectedRevision: Option[Int], metadata: Metadata, withReturn: BiFunction[ET, Integer, R]): Future[RepoUpdate[R]] = {
    repo.update(id, expectedRevision) {
      case (entity, revision) =>
        try Future successful withReturn(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }(metadata).map(t => RepoUpdate(t._1, t._2))(PiggyBack)
  }
  def updateReturn[R](id: ID, expectedRevision: Option[Int], withReturn: BiFunction[ET, Integer, R]): Future[RepoUpdate[R]] = {
    repo.update(id, expectedRevision) {
      case (entity, revision) =>
        try Future successful withReturn(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }(Metadata.empty).map(t => RepoUpdate(t._1, t._2))(PiggyBack)
  }
  def updateReturn[R](id: ID, metadata: Metadata, withReturn: BiFunction[ET, Integer, R]): Future[RepoUpdate[R]] = {
    repo.update(id) {
      case (entity, revision) =>
        try Future successful withReturn(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }(metadata).map(t => RepoUpdate(t._1, t._2))(PiggyBack)
  }

  def updateAsync[R](id: ID, expectedRevision: Option[Int], metadata: Metadata, withReturn: BiFunction[ET, Integer, Future[R]]): Future[RepoUpdate[R]] = {
    repo.update(id, expectedRevision) {
      case (entity, revision) =>
        try withReturn(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }(metadata).map(t => RepoUpdate(t._1, t._2))(PiggyBack)
  }
  def updateAsync[R](id: ID, expectedRevision: Option[Int], withReturn: BiFunction[ET, Integer, Future[R]]): Future[RepoUpdate[R]] = {
    repo.update(id, expectedRevision) {
      case (entity, revision) =>
        try withReturn(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }(Metadata.empty).map(t => RepoUpdate(t._1, t._2))(PiggyBack)
  }
  def updateAsync[R](id: ID, metadata: Metadata, withReturn: BiFunction[ET, Integer, Future[R]]): Future[RepoUpdate[R]] = {
    repo.update(id) {
      case (entity, revision) =>
        try withReturn(entity, revision) catch {
          case NonFatal(th) => Future failed th
        }
    }(metadata).map(t => RepoUpdate(t._1, t._2))(PiggyBack)
  }

}

case class RepoUpdate[R](returned: R, newRevision: Int)

package blogging.tests

import blogging._

import delta.process._
import delta.read.MessageSourceSupport
import delta.read.impl.PrebuiltReadModel

import blogging.read.BlogPostQueryModel.Author
import blogging.read.BlogPostsProcessor.StoredState

import scala.concurrent._, duration._

import java.util.UUID
import java.util.concurrent.ScheduledExecutorService


class AuthorReadModel(
  store: StreamProcessStore[UUID, StoredState, StoredState],
  defaultReadTimeout: FiniteDuration,
  hub: UpdateHub[UUID, StoredState])(
  implicit
  scheduler: ScheduledExecutorService)
extends PrebuiltReadModel[AuthorID, UUID, Author](store.name, defaultReadTimeout)
with MessageSourceSupport[AuthorID, Author, Author] {
  private[this] val reader = store.asSnapshotReader {
    case (uuid, author: StoredState.Author) => author toModel AuthorID(uuid)
  }
  protected def updateSource = hub.withUpdate[Author]
  protected def readSnapshot(
      id: AuthorID)(
      implicit
      ec: ExecutionContext)
      : Future[Option[delta.Snapshot[Author]]] =
    reader.read(id)
  protected def readAgain(
      id: AuthorID,
      minRevision: Int,
      minTick: Long)(
      implicit
      ec: ExecutionContext)
      : Future[Option[delta.Snapshot[Author]]] =
    readSnapshot(id)
}

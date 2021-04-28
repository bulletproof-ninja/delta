package blogging.tests

import blogging._

import delta.process._
import delta.read.MessageSourceSupport
import delta.read.impl.PrebuiltReadModel

import blogging.read.BlogPostQueryModel.BlogPost
import blogging.read.BlogPostsProcessor.StoredState

import scala.concurrent._, duration._

import java.util.UUID
import java.util.concurrent.ScheduledExecutorService


class BlogPostReadModel(
  store: StreamProcessStore[UUID, StoredState, StoredState],
  defaultReadTimeout: FiniteDuration,
  hub: UpdateHub[UUID, StoredState])(
  implicit
  scheduler: ScheduledExecutorService)
extends PrebuiltReadModel[BlogPostID, UUID, BlogPost](
  store.name, defaultReadTimeout)
with MessageSourceSupport[BlogPostID, BlogPost, BlogPost] {
  private[this] val reader = store.asSnapshotReader {
    case (uuid, StoredState.BlogPost(authorID, authorName, headline, wordCount, tags, externalSites, Some(publishDate))) =>
      new BlogPost(BlogPostID(uuid), headline, wordCount, tags, externalSites, publishDate, authorID, authorName)
  }
  protected def updateSource = hub.withUpdate[BlogPost]
  protected def readSnapshot(
      id: BlogPostID)(
      implicit
      ec: ExecutionContext)
      : Future[Option[delta.Snapshot[BlogPost]]] =
    reader.read(id)
  protected def readAgain(
      id: BlogPostID,
      minRevision: Int,
      minTick: Long)(
      implicit
      ec: ExecutionContext)
      : Future[Option[delta.Snapshot[BlogPost]]] =
    readSnapshot(id)
}

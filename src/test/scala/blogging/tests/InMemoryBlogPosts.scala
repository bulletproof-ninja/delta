package blogging.tests

import blogging._
import blogging.read.BlogPostQueryModel

import scala.concurrent.Future

import java.time.{ DayOfWeek, Month, Year }

import scuff.Reduction

import blogging.read.BlogPostsProcessor.StoredState

import java.util.UUID
import delta._
import scala.concurrent.ExecutionContext
import delta.testing.InMemoryProcStore
import delta.process.UpdateSource
import delta.read._
import delta.read.impl.PrebuiltReadModel
import java.util.concurrent.ScheduledExecutorService

class InMemoryBlogPosts(
  processStore: InMemoryProcStore[UUID, StoredState, StoredState],
  updateSource: UpdateSource[UUID, StoredState])(
  implicit
  ec: ExecutionContext,
  scheduler: ScheduledExecutorService)
extends BlogPostQueryModel {

  val authors: ReadModel[AuthorID, Author] =
    new PrebuiltReadModel[AuthorID, UUID, Author]("authors")
    with MessageSourceSupport[AuthorID, Author, Author]
    with SnapshotReaderSupport[AuthorID, Author] {
      protected val updateSource = InMemoryBlogPosts.this.updateSource.withUpdate[Author]
      protected val snapshotReader = processStore.asSnapshotReader[Author] {
        case (id, author: StoredState.Author) => author toModel AuthorID(id)
      }
    }
  val blogPosts: ReadModel[BlogPostID, BlogPost] =
    new PrebuiltReadModel[BlogPostID, UUID, BlogPost]("blog-posts")
    with MessageSourceSupport[BlogPostID, BlogPost, BlogPost] {
      protected val updateSource = InMemoryBlogPosts.this.updateSource.withUpdate[BlogPost]
      protected val publishedBlogPosts = processStore.asSnapshotReader {
        case (id, blogPost: StoredState.BlogPost) => blogPost toModel BlogPostID(id)
      }
      protected def readSnapshot(id: BlogPostID)(implicit ec: ExecutionContext)
          : Future[Option[delta.Snapshot[BlogPost]]] =
        publishedBlogPosts
          .read(id.uuid)
          .map {
            _.flatMap {
              _.transpose
            }
          }
      protected def readAgain(id: BlogPostID, minRevision: Int, minTick: Long)(implicit ec: ExecutionContext)
          : Future[Option[delta.Snapshot[BlogPostQueryModel.BlogPost]]] =
        readSnapshot(id)

    }

  def query[R](
      matchAnyTag: Set[String],
      matchExternalDomains: Set[String],
      matchAuthor: AuthorID,
      matchPublishYear: Year,
      matchPublishMonth: Month,
      matchPublishWeekday: DayOfWeek)(
      consumer: Reduction[Snapshot[BlogPost], R])
      : Future[R] =
    Future {
      processStore
        .iterator
        .flatMap {
          case (id, Snapshot(bp: StoredState.BlogPost, rev, tick)) =>
            bp.toModel(BlogPostID(id)).map(Snapshot(_, rev, tick))
          case _ =>
            None
        }
        .filter {
          case Snapshot(blogPost, _, _) =>
            val bpExtDomains = blogPost.externalSites.map {
              _.getHost.split("\\.").reverse.take(2).reverse.mkString(".").toLowerCase
            }
            (matchAnyTag.isEmpty || matchAnyTag.exists(blogPost.tags.contains)) &&
            (matchExternalDomains.isEmpty || matchExternalDomains.map(_.toLowerCase).exists(bpExtDomains.contains)) &&
            (matchAuthor == null || matchAuthor == blogPost.authorId) &&
            (matchPublishYear == null || matchPublishYear.getValue == blogPost.publishDate.getYear) &&
            (matchPublishMonth == null || matchPublishMonth == blogPost.publishDate.getMonth) &&
            (matchPublishWeekday == null || matchPublishWeekday == blogPost.publishDate.getDayOfWeek)
        }
        .foreach(consumer.next)
      consumer.result()
    }

}

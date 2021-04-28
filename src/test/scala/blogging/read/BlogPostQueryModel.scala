package blogging.read

import blogging._

import java.time._
import java.net.URI

import scala.concurrent._

import scuff._

import delta.read.ReadModel

object BlogPostQueryModel {

  case class BlogPost(
    id: BlogPostID,
    headline: String,
    wordCount: Int,
    tags: Set[String],
    externalSites: Set[URI],
    publishDate: LocalDate,
    authorId: AuthorID,
    authorName: String)

  case class Author(
    id: AuthorID,
    name: String,
    contact: Option[EmailAddress],
    publishedPosts: Set[BlogPostID])

}

trait BlogPostQueryModel {

  type BlogPost = BlogPostQueryModel.BlogPost
  type Author = BlogPostQueryModel.Author

  def query[R](
      matchTags: Set[String] = Set.empty,
      matchExternalDomains: Set[String] = Set.empty,
      matchAuthor: AuthorID = null,
      matchPublishYear: Year = null,
      matchPublishMonth: Month = null,
      matchPublishWeekday: DayOfWeek = null)(
      consumer: Reduction[delta.Snapshot[BlogPost], R]): Future[R]

  def authors: ReadModel[AuthorID, Author]
  def blogPosts: ReadModel[BlogPostID, BlogPost]

}

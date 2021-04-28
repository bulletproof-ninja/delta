package blogging.read

import blogging._
import blogging.write.blogpost//.events._
import blogging.write.author//.events._

import delta._
import delta.process._

import java.net.URI
import java.time.LocalDate
import java.util.UUID

import scuff._
import scala.concurrent._


object BlogPostsProcessor {

  sealed abstract class StoredState
  object StoredState {
    case class BlogPost(
      authorID: AuthorID,
      authorName: String = "[N/A]",
      headline: String = "[N/A]", wordCount: Int = -1, tags: Set[String] = Set.empty,
      externalLinks: Set[URI] = Set.empty,
      publishDate: Option[LocalDate] = None)
    extends StoredState {

      def toModel(id: BlogPostID): Option[BlogPostQueryModel.BlogPost] =
        publishDate.map { publishDate =>
          BlogPostQueryModel.BlogPost(id, headline, wordCount, tags, externalLinks, publishDate, authorID, authorName)
        }

    }

    case class Author(
      name: String = "[N/A]", emailAddrs: Set[EmailAddress] = Set.empty, publishedPosts: Set[BlogPostID] = Set.empty)
    extends StoredState {
      def toModel(id: AuthorID) =
        BlogPostQueryModel.Author(id, name, emailAddrs.headOption, publishedPosts)
    }
  }

  type Store = StreamProcessStore[UUID, StoredState, StoredState]

  type Hub = UpdateHub[UUID, StoredState]

  object StoredStateJSONCodec extends scuff.Codec[StoredState, JSON] {
    import scuff.json._
    import StoredState._
    def encode(state: StoredState): JSON =
      JsVal(state).toJson

    def decode(b: JSON): StoredState = {
      val ast = JsVal.parse(b).asObj
      ast.authorID match {
        case JsUndefined =>
          Author(
            name = ast.name.asStr,
            emailAddrs = ast.emailAddrs.asArr.map(_.asStr.value).map(EmailAddress(_)).toSet,
            publishedPosts = ast.publishedPosts.asArr.map(_.asStr.value).map(UUID.fromString).map(BlogPostID(_)).toSet)
        case JsStr(uuid) =>
          val authorID = AuthorID(UUID fromString uuid)
          BlogPost(
            authorID = authorID,
            authorName = ast.authorName.asStr,
            headline = ast.headline.asStr,
            wordCount = ast.wordCount.asNum,
            tags = ast.tags.asArr.map(_.asStr.value).toSet,
            publishDate = ast.get("publishDate").map(_.asStr.value).map(LocalDate.parse),
            externalLinks = ast.get("externalLinks").map(_.asArr.map(_.asStr.value).map(URI.create).toSet) getOrElse Set.empty)
        case unexpected => sys.error(s"""Unexpected "author" type: $unexpected""")
      }
    }
  }


}

import BlogPostsProcessor._

class BlogPostsProcessor(
  updateHub: Hub,
  protected val processStore: StreamProcessStore[UUID, StoredState, StoredState])(
  implicit
  protected val adHocExeCtx: ExecutionContext)
extends SimpleIdempotentJoinConsumer[UUID, BloggingEvent, StoredState] {
  import StoredState._
  protected def EnrichmentEvaluator(
      refTx: Transaction, refState: StoredState) = refState match {
    case blogPost: BlogPost => enrichAuthorFrom(BlogPostID(refTx.stream), blogPost)
    case author: Author => enrichBlogPostFrom(AuthorID(refTx.stream), author)
  }

  private def enrichAuthorFrom(blogPostID: BlogPostID, blogPost: BlogPost): EnrichmentEvaluator = {
    case _: blogpost.events.BlogPostPublished =>
      Enrichment(blogPost.authorID, new Author) { author =>
        author.copy(publishedPosts = author.publishedPosts + blogPostID)
      }
  }

  private def enrichBlogPostFrom(authorID: AuthorID, auth: Author): EnrichmentEvaluator = {
    case author.events.AuthorNameChanged(newName) =>
      Enrichment(auth.publishedPosts, new BlogPost(authorID)) { blogPost =>
        blogPost.copy(authorName = newName)
      }
  }

  protected def selector(es: EventSource): es.Selector = {
    import blogging.write.author.Author
    import blogging.write.blogpost.BlogPost

    es.ChannelSelector(Author.channel, BlogPost.channel)
  }

  protected def process(
      tx: Transaction, currState: Option[StoredState])
      : Future[StoredState] =
    tx.channel match {
      case blogpost.BlogPost.channel =>
        BlogPostProjector(tx, currState)
      case author.Author.channel =>
        AuthorProjector(tx, currState)
      case _ =>
        sys.error("Check selector")
    }

  protected def reportFailure(th: Throwable): Unit =
    th.printStackTrace(System.err)

  protected def onUpdate(id: UUID, update: Update): Unit =
    updateHub.publish(id, update)

}

import blogpost.events._

object BlogPostProjector
extends Projector[StoredState.BlogPost, blogpost.events.BlogPostEvent] {
  import StoredState.BlogPost

  private val FindWords = """
  \w+
  """.trim.r

  def init(evt: BlogPostEvent)  =
    evt dispatch new Handler

  def next(state: BlogPost, evt: BlogPostEvent) =
    evt dispatch new Handler(state)

  private class Handler(blogPost: BlogPost = null)
  extends BlogPostEvents {
    type Return = BlogPost

    def on(evt: BlogPostDrafted) = {
      require(blogPost == null)
      val wordCount = FindWords.findAllMatchIn(evt.body).size
      new BlogPost(evt.author, headline = evt.headline, wordCount = wordCount, tags = evt.tags)
    }

    def on(evt: BlogPostPublished) =
      blogPost.copy(publishDate = Some(evt.dateTime.toLocalDate), externalLinks = evt.externalLinks)

    def on(evt: TagsUpdated) =
      blogPost.copy(tags = evt.tags)

  }

}

object AuthorProjector
extends Projector[StoredState.Author, author.events.AuthorEvent] {
  import StoredState.Author
  import author.events._

  def init(evt: AuthorEvent) =
    evt dispatch new EventHandler
  def next(state: Author, evt: AuthorEvent) =
    evt dispatch new EventHandler(state)

  private class EventHandler(author: Author = new Author)
  extends AuthorEvents {
    type Return = Author

    def on(evt: AuthorRegistered) = {
      author.copy(
        name = evt.name,
        emailAddrs = author.emailAddrs + evt.emailAddress)
    }

    def on(evt: AuthorNameChanged) =
      author.copy(name = evt.newName)

    def on(evt: EmailAddressAdded) =
      author.copy(emailAddrs = author.emailAddrs + evt.emailAddress)
    def on(evt: EmailAddressRemoved) =
      author.copy(emailAddrs = author.emailAddrs - evt.emailAddress)

  }
}

class EnrichAuthor(
  blogPostID: BlogPostID,
  blogPost: StoredState.BlogPost)
extends BlogPostEvents {
  import StoredState.Author
  import JoinState._

  type Return = Enrichment[UUID, Author]

  def on(evt: BlogPostDrafted) = Enrichment.empty

  def on(evt: BlogPostPublished) =
    Enrichment(blogPost.authorID, new Author) { author =>
      author.copy(publishedPosts = author.publishedPosts + blogPostID)
    }

  def on(evt: TagsUpdated) = Enrichment.empty

}

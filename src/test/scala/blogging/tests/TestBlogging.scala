package blogging.tests

import blogging._
import blogging.read._
import blogging.read.BlogPostsProcessor.StoredState
import blogging.write.{ author, blogpost, newMetadata }
import blogging.write.author._
import blogging.write.blogpost._

import java.util.concurrent.Executors

import delta._
import delta.util.TransientEventStore
import delta.util.LocalTransport
import delta.testing.InMemoryProcStore
import delta.process._, LiveProcessConfig._

import scuff.{ LamportClock, EmailAddress }
import scuff.concurrent._

import scala.concurrent._, duration._
import scala.util.Random

import java.time._

import delta.testing.BaseTest
import java.util.UUID
import java.net.URI

object TestBlogging {

  val Scheduler = Executors.newSingleThreadScheduledExecutor()

}

abstract class TestBlogging
extends BaseTest {

  type JSON = String

  // Enable stack traces, always
  sys.SystemProperties.noTraceSuppression setValue true

  implicit def Scheduler = TestBlogging.Scheduler

  type Scenario
  def scenarios: Iterable[Scenario]

  val lamportClock = new LamportClock(0)

  def newEventStore(clock: LamportClock)(implicit scenario: Scenario): BloggingEventStore
  type ProcStore <: BlogPostsProcessor.Store

  def newProcessStore(name: String)(implicit scenario: Scenario): ProcStore

  def newBlogPostQueryModel(
      store: ProcStore,
      defaultReadTimeout: FiniteDuration,
      hub: UpdateHub[UUID, StoredState])(
      implicit
      scenario: Scenario)
      : BlogPostQueryModel

  def numAuthors = 20
  def numBlogPosts = 750
  val timeout = 10.minutes


  test("comprehensive test") {
    scenarios.foreach(comprehensiveTest(_))
  }

  private def comprehensiveTest(implicit s: Scenario): Unit = {
    val subjects = Array("food", "politics", "cars", "music", "art", "science")
    val externalSites = Array("https://medium.com", "https://www.tumblr.com", "https://www.blogger.com")
    val es = newEventStore(lamportClock)
    val authorRepo = new author.Repository(es)
    val blogPostRepo = new blogpost.Repository(es)
    val authors = {
      val authors = Future sequence (1 to numAuthors).map { _ =>
        val name = s"${Monkey.nextWord(true)} ${Monkey.nextWord(true)}"
        val emailAddress = EmailAddress(name.toLowerCase.replace(' ', '.'), "foo-mail.com")
        val author = Author apply RegisterAuthor(name, emailAddress)
        authorRepo.insert(AuthorID.newID(), author)
      }
      authors.await(timeout)
    }
    locally {
      Future sequence
        authors
          .map { authorId =>
            authorRepo.update(authorId) {
              case (author, _) =>
                val oldEmail = author.emailAddresses.head
                val newEmail = EmailAddress(oldEmail.user, "bar-email.net")
                author(UpdateEmailAddress(from = oldEmail, to = newEmail))
            }
          }
    }.await(timeout)

    case class Drafted(id: BlogPostID, wordCount: Int, tags: Set[String], title: String)
    val blogPostDrafts = {
      val blogPosts = Future sequence (1 to numBlogPosts).map { _ =>
        val title = Monkey.generateHeadline()
        val text = Monkey.generateBlogText(Random.between(500, 5000))
        val authorID = authors(Random.nextInt(authors.length))
        val tags = (1 to Random.between(1, subjects.length)).map { _ =>
          subjects(Random.between(0, subjects.length))
        }.toSet
        assert(tags.nonEmpty)
        val blogPost = BlogPost(DraftBlogPost(authorID, title, text, tags))
        blogPostRepo.insert(BlogPostID.newID(), blogPost).map {
          Drafted(_, text.split("[ ,.]+").length, tags, title)
        }
      }
      blogPosts.await(timeout)
    }

    val blogPostsProcStore = newProcessStore("blog-post-queries")
    val blogPostsTopic = MessageTransport.Topic(s"update:${blogPostsProcStore.name}")
    val transport = new LocalTransport[(UUID, delta.process.Update[StoredState])](ec)
    val hub: BlogPostsProcessor.Hub = MessageHub(transport, blogPostsTopic)
    val blogPostQueryModel = newBlogPostQueryModel(blogPostsProcStore, 20.seconds, hub)

    val processor =
      new BlogPostsProcessor(hub, blogPostsProcStore)

    val replayProcess =
      processor.start(
        es,
        ReplayProcessConfig(completionTimeout = timeout, writeBatchSize = 10),
        LiveProcessConfig(ImmediateReplay))
    val (replayResult, liveProcess) = replayProcess.finished.await(timeout)
    assert(replayResult.processErrors.isEmpty)

    val oneSubject = blogPostQueryModel.query(
      matchTags = Set(subjects.head)) {
        new BlogPostConsumer(_.values.map(_.state))
      }.await(timeout)

    assert(oneSubject.isEmpty) // All drafts, nothing published

    case class Published(id: BlogPostID, revision: Revision, publishDate: LocalDate, wordCount: Int, tags: Set[String])
    val publishedBlogPosts = {
      Future sequence
        blogPostDrafts
          .zipWithIndex
          .filter(_._2 % 5 == 0) // Publish every 5th draft
          .map(_._1)
          .map {
            case Drafted(id, wordCount, tags, title) =>
              val publishedURLs = (0 to Random.between(0, externalSites.length + 1)).map { _ =>
                val extSite = externalSites(Random.between(0, externalSites.length))
                val articleId = (' ' :: ',' :: '.' :: Nil).foldLeft(title.toLowerCase) {
                  case (path, invalid) => path.replace(invalid, '-')
                }
                new URI(s"$extSite/${tags.headOption getOrElse subjects(0)}/$articleId")
              }
              val year = Random.between(Year.now.getValue - 15, Year.now.getValue)
              val month = Random.between(1, 13)
              val day = month match {
                case 2 => Random.between(1, 29)
                case 4 | 6 | 9 | 11 => Random.between(1, 31)
                case _ => Random.between(1, 32)
              }
              val date = LocalDate.of(year, month, day)
              val dateTime = LocalDateTime.of(date, LocalTime.now)
              blogPostRepo.update(id) {
                case (blogPost, _) =>
                  blogPost(PublishBlogPost(dateTime, publishedURLs.toSet))
              }
              .map { revision =>
                Published(id, revision, date, wordCount, tags)
              }
        }
    }.await(timeout)

    val randomWeekDay = publishedBlogPosts.head.publishDate.getDayOfWeek

    val onRandomWeekDay = {
      Future sequence
        publishedBlogPosts
        .map { bp =>
          blogPostQueryModel.blogPosts.read(bp.id, minRevision = bp.revision) // Ensure publish events have been processed.
            .map { snapshot =>
              assert(bp.revision === snapshot.revision)
              if (snapshot.state.publishDate.getDayOfWeek == randomWeekDay) Some(snapshot.state.id)
              else None
            }
        }
    }.await(timeout).toSet.flatten

    val weekDayQueryResult = blogPostQueryModel.query(matchPublishWeekday = randomWeekDay) {
      new BlogPostConsumer(_.keySet.toSet)
    }.await(timeout)

    assert(onRandomWeekDay === weekDayQueryResult)

    val selectedAuthorId = authors.head

    val byAuthorQueryResult = blogPostQueryModel.query(matchAuthor = selectedAuthorId) {
      new BlogPostConsumer(identity)
    }.await(timeout)

    val maxTick = byAuthorQueryResult.values.map(_.tick).max

    val publishCount = byAuthorQueryResult.size

    var retries = 10
    var authorPublishCount = 0
    while (retries > 0 && publishCount != authorPublishCount) {
      val author0 = blogPostQueryModel.authors.read(selectedAuthorId, maxTick).await(timeout).state
      authorPublishCount = author0.publishedPosts.size
      if (publishCount != authorPublishCount) Thread sleep 777
      retries -= 1
    }

    assert(publishCount == authorPublishCount)

    val matchTags = subjects.take(2).toSet
    val multiSelectionQuery =
      blogPostQueryModel.query(
        matchTags = matchTags,
        matchExternalDomains = Set("medium.com"),
        matchPublishYear = Year of 2020) {
          new BlogPostConsumer(_.values.map(_.state))
      }.await(timeout)

    assert(multiSelectionQuery.nonEmpty)

    multiSelectionQuery.foreach { blogPost =>
      assert(matchTags.exists(blogPost.tags.contains))
      assert(blogPost.externalSites.exists(_.toString contains "medium.com"), s"Blogpost `${blogPost.id}` was expected to have `medium.com` link: ${blogPost.externalSites}")
      assertResult(2020){ blogPost.publishDate.getYear }
    }

    liveProcess.cancel()
  }
}

class InMemTestBlogging
extends TestBlogging {

  type Scenario = Unit
  def scenarios: Iterable[Unit] = () :: Nil

  def newEventStore(clock: LamportClock)(implicit scenario: Scenario): BloggingEventStore =
    new TransientEventStore[UUID, BloggingEvent, JSON](
      ec, JsonEventFormat)
    with BloggingEventStore {
      val ticker = LamportTicker(clock)
    }

  type ProcStore = InMemoryProcStore[UUID, StoredState, StoredState]

  def newProcessStore(
      name: String)(implicit scenario: Scenario)
      : ProcStore =
    new InMemoryProcStore(name)

  def newBlogPostQueryModel(
      store: ProcStore,
      timeout: FiniteDuration,
      hub: UpdateHub[UUID, StoredState])(implicit scenario: Scenario)
      : BlogPostQueryModel =
    store match {
      case store: InMemoryProcStore[UUID, StoredState, StoredState] =>
        new InMemoryBlogPosts(store, hub)
      case _ => ???
    }

}

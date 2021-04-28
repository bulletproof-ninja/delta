package blogging.tests.mongo

import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import blogging._
import blogging.read._, BlogPostsProcessor.StoredState

import blogging.tests.JsonEventFormat
import blogging.tests.TestBlogging
import blogging.tests._

import delta.LamportTicker
import delta.Ticker
import delta.process._
import delta.mongo._

import scuff.Codec
import scuff.LamportClock
import delta.Snapshot
import java.time.{DayOfWeek, Month, Year}
import scuff.Reduction
import delta.read.ReadModel
import java.time.LocalDate
import java.net.URI
import org.bson._

class MongoTestBlogging
extends TestBlogging
with delta_testing.MongoCollections {

  type Scenario = Unit
  def scenarios: Iterable[Unit] = () :: Nil

  def newEventStore(clock: LamportClock)(implicit scenario: Scenario): BloggingEventStore = {
    val collection = MongoEventStore.getCollection(ns, settings, client)
    val evtFmt = JsonEventFormat adapt BsonJsonCodec
    new MongoEventStore[UUID, BloggingEvent](
      collection, evtFmt)
    with BloggingEventStore {
      def publishCtx = ec
      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
        subscribeLocal(selector)(callback)
      val ticker: Ticker = LamportTicker(clock)
    }
  }.ensureIndexes()

  implicit val storedStateCodec = {
    val bsonCodec =
      BlogPostsProcessor.StoredStateJSONCodec pipe BsonJsonCodec pipe new Codec[BsonValue, BsonValue] {
        def encode(a: BsonValue): BsonValue = {
          val doc = a.asDocument
          Option(doc.get("publishDate")).map(_.asString.getValue).map(LocalDate.parse) foreach {
            case publishDate =>
              val dow = publishDate.getDayOfWeek.toString.substring(0, 3)
              doc
                .append("publishYear", publishDate.getYear)
                .append("publishMonth", publishDate.getMonthValue)
                .append("publishWeekDay", dow)
          }
          Option(doc.get("externalLinks")).map(_.asArray.asScala.toList.map(_.asString.getValue)) foreach {
            case externalLinks =>
              if (externalLinks.nonEmpty) {
                val externalDomains =
                  externalLinks
                    .map(URI.create)
                    .map(_.getHost.split("\\.").reverse.take(2).reverse.mkString("."))
                    .map(new BsonString(_))
                doc.append("externalDomains", new BsonArray(externalDomains.asJava))
              }
          }
          doc
        }

        def decode(b: BsonValue): BsonValue = b
      }
    SnapshotCodec[StoredState](bsonCodec) {
      case _: StoredState.BlogPost => write.blogpost.BlogPost.channel.toString
      case _: StoredState.Author => write.author.Author.channel.toString
    }
  }

  type ProcStore = MongoProcStore

  object MongoProcStore {
    def apply(name: String) =
      new MongoProcStore(name).ensureIndexes()
  }

  private object BlogPostFields extends Enumeration {
    val
      tags,
      externalDomains,
      authorID,
      publishYear,
      publishMonth,
      publishWeekDay = Value
  }

  private val blogPostField = write.blogpost.BlogPost.channel.toString
  private def blogPost(name: BlogPostFields.Value) = s"$blogPostField.$name"
  private val blogPostIndexFields = BlogPostFields.values.toSeq.map(blogPost)

  class MongoProcStore private (name: String)
  extends MongoStreamProcessStore[UUID, StoredState, StoredState](
    getCollection(name),
    blogPostIndexFields: _*) {

    private def bson(str: String) = new BsonString(str)
    private def bson(int: Int) = new BsonInt32(int)
    private def bson(uuid: UUID) = new BsonString(uuid.toString)
    private def bson(dow: DayOfWeek) = new BsonString(dow.toString.substring(0, 3))

    def query[R](
        matchTags: Set[String],
        matchExternalDomains: Set[String],
        matchAuthor: blogging.AuthorID,
        matchPublishYear: Year,
        matchPublishMonth: Month,
        matchPublishWeekday: DayOfWeek)(
        consumer: Reduction[delta.Snapshot[BlogPostQueryModel.BlogPost], R]): Future[R] = {

      val filter: List[(String, BsonValue)] = {
        val filter: List[IterableOnce[(String, BsonValue)]] = {
          val bpf = BlogPostFields
          matchTags.toList.map { tag => blogPost(bpf.tags) -> bson(tag)} ::
          matchExternalDomains.toList.map { domain => blogPost(bpf.externalDomains) -> bson(domain) } ::
          Option(matchAuthor).map(_.uuid).map(bson(_)).map(blogPost(bpf.authorID) -> _) ::
          Option(matchPublishYear).map(_.getValue).map(bson(_)).orElse(Some($exists())).map(blogPost(bpf.publishYear) -> _) ::
          Option(matchPublishMonth).map(_.getValue).map(bson(_)).map(blogPost(bpf.publishMonth) -> _) ::
          Option(matchPublishWeekday).map(bson(_)).map(blogPost(bpf.publishWeekDay) -> _) ::
          Nil
        }
        filter.map(_.iterator.to(List)).flatten
      }

      bulkRead(filter: _*) {
        new BlogPostConsumerProxy(consumer)
      }

    }
  }

  def newProcessStore(name: String)(implicit scenario: Scenario) =
    MongoProcStore(name)

  def newBlogPostQueryModel(
      store: ProcStore,
      defaultReadTimeout: FiniteDuration,
      hub: UpdateHub[UUID,BlogPostsProcessor.StoredState])(
      implicit
      scenario: Scenario)
      : BlogPostQueryModel =
    new BlogPostQueryModel {

      def query[R](
          matchTags: Set[String],
          matchExternalDomains: Set[String],
          matchAuthor: blogging.AuthorID.Type,
          matchPublishYear: Year,
          matchPublishMonth: Month,
          matchPublishWeekday: DayOfWeek)(
          consumer: Reduction[Snapshot[BlogPost], R]): Future[R] =
      store.query(
        matchTags, matchExternalDomains, matchAuthor,
        matchPublishYear, matchPublishMonth, matchPublishWeekday)(
          consumer)

      lazy val authors: ReadModel[AuthorID, Author] =
        new AuthorReadModel(store, defaultReadTimeout, hub)

      lazy val blogPosts: ReadModel[BlogPostID, BlogPost] =
        new BlogPostReadModel(store, defaultReadTimeout, hub)


    }


}

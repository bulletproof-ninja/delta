package blogging.tests.jdbc

import java.sql.ResultSet
import java.time._
import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import blogging._
import blogging.read._, BlogPostsProcessor.StoredState
import blogging.tests._

import delta._
import delta.jdbc.{ JavaEnumColumn => _, _ }
import delta.read._
import scuff._
import scuff.jdbc.AsyncConnectionSource

import JdbcStreamProcessStore._
import delta.util.LocalTransport

object JdbcTestBlogging {

  implicit object WeekDayColumn
  extends ColumnType[DayOfWeek] {
    final val len = 3
    def stringValue(dow: DayOfWeek) = dow.name.substring(0, len)
    private[this] val lookup = DayOfWeek.values.map { dow =>
      stringValue(dow) -> dow
    }.toMap
    val sqlColumn = CharColumn(len)
    def typeName: String = sqlColumn.typeName
    override def writeAs(dow: DayOfWeek): AnyRef =
      sqlColumn writeAs stringValue(dow)
    def readFrom(rs: ResultSet, col: Int): DayOfWeek =
      lookup(sqlColumn.readFrom(rs, col))
  }

}

abstract class JdbcTestBlogging
extends blogging.tests.TestBlogging {

  type Scenario = Option[Short] // Versioned
  def scenarios: Iterable[Option[Short]] = None :: Some(42: Short) :: Nil

  import blogging.tests.jdbc.JdbcTestBlogging._

  implicit def UUIDColumn: ColumnType[UUID]

  def JSONColumn: ColumnType[JSON]

  implicit def StoredStateColumn = JSONColumn adapt BlogPostsProcessor.StoredStateJSONCodec

  import StoredState._

  val typeIndex = Index {
    NotNull[StoredState, String]("stream_type") {
      case _: BlogPost => blogging.write.blogpost.BlogPost.channel.toString
      case _: Author => blogging.write.author.Author.channel.toString
    }(ShortString)
  }

  val authorIndex = Index {
    Nullable[StoredState, UUID]("blogpost_author") {
      case blogPost: BlogPost => blogPost.authorID.uuid
    }
  }
  implicit def shortColumn: ColumnType[Short]
  val yearIndex = Index {
    Nullable[StoredState, Short]("publish_year") {
      case BlogPost(_, _, _, _, _, _, Some(publishDate)) => publishDate.getYear.toShort
    }
  }
  implicit def byteColumn: ColumnType[Byte]
  val monthIndex = Index {
    Nullable[StoredState, Byte]("publish_month") {
      case BlogPost(_, _, _, _, _, _, Some(publishDate)) => publishDate.getMonthValue.toByte
    }
  }
  val weekDayIndex = Index {
    Nullable[StoredState, DayOfWeek]("publish_weekday") {
      case BlogPost(_, _, _, _, _, _, Some(publishDate)) => publishDate.getDayOfWeek
    }(WeekDayColumn)
  }

  import IndexTableSupport._

  def ShortString: ColumnType[String]

  val tagsIndexTable = {
    implicit def str = ShortString
    IndexTable[StoredState, String]("tag") {
      case bp: BlogPost => bp.tags
    }
  }
  val extDomainIndexTable = {
    implicit def str = ShortString
    IndexTable[StoredState, String]("external_domain") {
      case bp: BlogPost =>
        bp.externalLinks
          .map {
            _.getHost
              .split("\\.")
              .reverse.take(2).reverse
              .mkString(".").toLowerCase
          }
    }
  }



  protected def connSource: AsyncConnectionSource

  class JdbcProcStore(name: String, schema: Option[String], version: Option[Short])
  extends JdbcStreamProcessStore[UUID, StoredState, StoredState](
    Table(name.replace("-", "_") concat (if (version.isEmpty) "" else "_x"), schema = schema, version = version),
    typeIndex, authorIndex, yearIndex, monthIndex, weekDayIndex)
  with IndexTableSupport[UUID, StoredState, StoredState] {
    def this(name: String, schema: String, version: Option[Short]) = this(name, Option(schema), version)
    protected def connectionSource: AsyncConnectionSource = connSource
    protected val indexTables =
      extDomainIndexTable ::
      tagsIndexTable ::
      Nil

    private[this] val onlyBlogPosts = {
      implicit def col = ShortString
      "stream_type" -> QueryValue(blogging.write.blogpost.BlogPost.channel.toString)
    }

    def query[R](
        matchTags: Set[String],
        matchExternalDomains: Set[String],
        matchAuthor: AuthorID,
        matchPublishYear: Year,
        matchPublishMonth: Month,
        matchPublishWeekday: DayOfWeek)(
        consumer: Reduction[delta.Snapshot[BlogPostQueryModel.BlogPost], R])
        : Future[R] = {

      val filter: List[(String, QueryValue)] = {
        implicit def strCol = ShortString
        val filter: List[Option[(String, QueryValue)]] = {
          matchTags.headOption.map(_ => "tag" -> QueryValue(matchTags)) ::
          matchExternalDomains.headOption.map(_ => "external_domain" -> QueryValue(matchExternalDomains)) ::
          Option(matchAuthor).map(_.uuid).map(QueryValue(_)).map("blogpost_author" -> _) ::
          Option(matchPublishYear).map(_.getValue.toShort).map(QueryValue(_)).orElse(Some(NOT_NULL)).map("publish_year" -> _) ::
          Option(matchPublishMonth).map(_.getValue.toByte).map(QueryValue(_)).map("publish_month" -> _) ::
          Option(matchPublishWeekday).map(QueryValue(_)).map("publish_weekday" -> _) ::
          Nil
        }
        onlyBlogPosts :: filter.map(_.toList).flatten
      }

      bulkRead(filter: _*) {
        new BlogPostConsumerProxy(consumer)
      }
    }

  }

  type ProcStore = JdbcProcStore

  def dialect(implicit version: Option[Short]): Dialect[UUID, BloggingEvent, JSON]
  trait TableNameOverride
  extends Dialect[UUID, BloggingEvent, JSON] {
    def isVersioned: Boolean
    private def suffix = if (isVersioned) "_x" else ""
    override protected def streamTable = s"${super.streamTable}$suffix"
    override protected def transactionTable = s"${super.transactionTable}$suffix"
    override protected def eventTable = s"${super.eventTable}$suffix"
    override protected def metadataTable = s"${super.metadataTable}$suffix"
  }

  def newEventStore(clock: LamportClock)(implicit scenario: Scenario): BloggingEventStore =
    new JdbcEventStore[UUID, BloggingEvent, JSON](
      JsonEventFormat,
      dialect,
      connSource)
    with BloggingEventStore {
      protected val txTransport = new LocalTransport[Transaction](ec)
      // def txTransportCodec = Codec.noop
      val ticker: Ticker = LamportTicker(clock)
      def publishCtx = ec
      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
        subscribeLocal(selector)(callback)

    }.ensureSchema()

  def newProcessStore(name: String)(implicit version: Option[Short]): ProcStore

  def newBlogPostQueryModel(
      store: ProcStore,
      defaultReadTimeout: FiniteDuration,
      hub: delta.process.UpdateHub[UUID, BlogPostsProcessor.StoredState])(
      implicit
      version: Option[Short])
      : BlogPostQueryModel =
    new BlogPostQueryModel {

      def query[R](
          matchTags: Set[String],
          matchExternalDomains: Set[String],
          matchAuthor: blogging.AuthorID,
          matchPublishYear: Year,
          matchPublishMonth: Month,
          matchPublishWeekday: DayOfWeek)(
          consumer: Reduction[Snapshot[BlogPost], R])
          : Future[R] =
        store.query(matchTags, matchExternalDomains, matchAuthor, matchPublishYear, matchPublishMonth, matchPublishWeekday)(consumer)

      val authors: ReadModel[AuthorID, Author] =
        new AuthorReadModel(store, defaultReadTimeout, hub)

      val blogPosts: ReadModel[BlogPostID, BlogPost] =
        new BlogPostReadModel(store, defaultReadTimeout, hub)

    }

}

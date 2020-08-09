package delta.cassandra

import java.util.{ ArrayList, List => JList }
import java.lang.{ Byte => JByte }

import scala.{ Left, Right }
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import com.datastax.driver.core._

import scuff.{ Memoizer, StreamConsumer }
import scuff.concurrent._

import delta._

private[cassandra] object CassandraEventStore {

  private def ensureTable[ID: ColumnType, SF: ColumnType](
      session: Session, keyspace: String, table: String, replication: Map[String, Any]): Unit = {
    val replicationStr = replication.map {
      case (key, str: CharSequence) => s"'$key':'$str'"
      case (key, cls: Class[_]) => s"'$key':'${cls.getName}'"
      case (key, any) => s"'$key':$any"
    }.mkString("{", ",", "}")
      def cqlName[T: ColumnType]: String = implicitly[ColumnType[T]].typeName
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStr")
    session.execute(s"""
      CREATE TABLE IF NOT EXISTS $keyspace.$table (
        stream_id ${cqlName[ID]},
        revision INT,
        tick BIGINT,
        channel TEXT STATIC,
        event_names LIST<TEXT>,
        event_versions LIST<TINYINT>,
        event_data LIST<${cqlName[SF]}>,
        metadata MAP<TEXT,TEXT>,
        PRIMARY KEY ((stream_id), revision)
      ) WITH CLUSTERING ORDER BY (revision ASC)""")
    session.execute(s"""CREATE INDEX IF NOT EXISTS ON $keyspace.$table(channel)""")
  }

  private val StreamColumns = Seq("revision", "tick", "channel", "event_names", "event_versions", "event_data", "metadata")
  private val TxColumns = "stream_id" +: StreamColumns
  private def streamColumns: String = StreamColumns.mkString(",")
  private def txColumns: String = TxColumns.mkString(",")
  private val StreamColumnsIdx = Columns(StreamColumns.indexOf(_))
  private val TxColumnsIdx = Columns(TxColumns.indexOf(_))

  private case class Columns(
      stream_id: Int, revision: Revision,
      tick: Int, channel: Int,
      event_names: Int, event_versions: Int, event_data: Int,
      metadata: Int)
  private object Columns {
    def apply(colIdx: String => Int): Columns = {
      new Columns(
        stream_id = colIdx("stream_id"),
        revision = colIdx("revision"),
        tick = colIdx("tick"),
        channel = colIdx("channel"),
        event_names = colIdx("event_names"),
        event_versions = colIdx("event_versions"),
        event_data = colIdx("event_data"),
        metadata = colIdx("metadata"))
    }
  }
}

trait TableDescriptor {
  def keyspace: String
  def table: String
  def replication: Map[String, Any]
}

/**
 * Cassandra event store implementation.
 * WARNING: Not tested to any appreciable degree.
 * @param exeCtx The internal execution context
 * @param session The Cassandra session (connection pool)
 * @param td The table descriptor
 */
abstract class CassandraEventStore[ID: ColumnType, EVT, SF: ColumnType](
  session: Session, td: TableDescriptor,
  evtFmt: EventFormat[EVT, SF])(
  initTicker: CassandraEventStore[ID, EVT, SF] => Ticker)(
  implicit
  ec: ExecutionContext)
extends EventStore[ID, EVT] {

  lazy val ticker = initTicker(this)

  import CassandraEventStore._

  /** Create table definition, if not already exists. */
  def ensureTable(ensureTable: Boolean = true): this.type = {
    if (ensureTable) {
      CassandraEventStore.ensureTable[ID, SF](session, td.keyspace, td.table, td.replication)
    }
    this
  }

  private def ct[T: ColumnType] = implicitly[ColumnType[T]]

  private[this] val TableName = s"${td.keyspace}.${td.table}"

  protected def toTransaction(knownStream: Option[ID], row: Row, columns: Columns): Transaction = {
    val stream = knownStream getOrElse ct[ID].readFrom(row, columns.stream_id)
    val tick = row.getLong(columns.tick)
    val channel = Channel(row.getString(columns.channel))
    val revision = row.getInt(columns.revision)
    val metadata = {
      val map = row.getMap(columns.metadata, classOf[String], classOf[String])
      if (map.isEmpty) {
        Map.empty[String, String]
      } else {
        map.asScala.toMap
      }
    }
    val eventNames = row.getList(columns.event_names, classOf[String])
    val eventVersions = row.getList(columns.event_versions, classOf[JByte])
    val eventData = row.getList(columns.event_data, ct[SF].jvmType)
    val events = fromJLists(channel, metadata)(eventNames, eventVersions, eventData)
    Transaction(tick, channel, stream, revision, metadata, events)
  }

  private def execute[T](stm: BoundStatement)(handler: ResultSet => T): Future[T] = {
    val result = session.executeAsync(stm)
    val promise = Promise[T]()
    val listener = new Runnable {
      def run: Unit = promise complete Try(handler(result.get))
    }
    result.addListener(listener, ec)
    promise.future
  }

  // TODO: Make fully callback driven:
  private def processMultiple(callback: Transaction => Unit, stms: Seq[BoundStatement]): Future[Unit] = {
      def iterate(rss: Seq[ResultSet]): Seq[ResultSet] = {
        for {
          rs <- rss
          _ <- 0 until rs.getAvailableWithoutFetching
        } {
          callback(toTransaction(None, rs.one, TxColumnsIdx))
        }
        rss.filterNot(_.isExhausted)
      }

    val resultSets = stms.map { stm =>
      execute(stm)(identity)
    }
    Future.sequence(resultSets).map { rss =>
      var remaining = rss
      while (remaining.nonEmpty) {
        remaining = iterate(remaining)
      }
    }
  }

  // TODO: Make fully non-blocking
  private def queryAsync[U](stream: Some[ID], callback: StreamConsumer[Transaction, U], stm: BoundStatement): Unit = {
    execute(stm) { rs =>
      Try {
        val iter = rs.iterator().asScala.map(row => toTransaction(stream, row, StreamColumnsIdx))
        while (iter.hasNext) callback onNext iter.next()
      } match {
        case Success(_) => callback.onDone()
        case Failure(NonFatal(cause)) => callback onError cause
      }
    }
  }

  private def fromJLists(channel: Channel, metadata: Map[String, String])(types: JList[String], vers: JList[JByte], data: JList[SF]): List[EVT] = {
    val size = types.size
    assert(vers.size == size && data.size == size)
    var idx = size - 1
    var list = List.empty[EVT]
    while (idx != -1) {
      val evt = evtFmt.decode(types.get(idx), vers.get(idx), data.get(idx), channel, metadata)
      list = evt :: list
      idx -= 1
    }
    list
  }
  private def toJLists(events: List[EVT]): (JList[String], JList[JByte], JList[SF]) = {
    val types = new ArrayList[String](8)
    val typeVers = new ArrayList[JByte](8)
    val data = new ArrayList[SF](8)
    events.foreach { evt =>
      val EventFormat.EventSig(name, version) = evtFmt signature evt
      types add name
      typeVers add version
      data add evtFmt.encode(evt)
    }
    (types, typeVers, data)
  }

  private lazy val CurrentRevision = {
    val ps = session.prepare(s"""
      SELECT revision
      FROM $TableName
      WHERE stream_id = ?
      ORDER BY revision DESC
      LIMIT 1""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID) => ps.bind(ct[ID].writeAs(id))
  }
  def currRevision(streamId: ID): Future[Option[Int]] = {
    execute(CurrentRevision(streamId))(rs => Option(rs.one).map(_.getInt(0)))
  }

  private lazy val GetLastTick = {
    val ps = session.prepare(s"SELECT MAX(tick) FROM $TableName")
    () => ps.bind()
  }
  def maxTick: Future[Option[Long]] =
    execute(GetLastTick()) { rs =>
      Option(rs.one).map(_.getLong(0))
    }

  private lazy val ReplayEverything: () => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txColumns
      FROM $TableName
      """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    () => ps.bind()
  }
  private lazy val ReplayEverythingSince: Long => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txColumns
      FROM $TableName
      WHERE tick >= ?
      ALLOW FILTERING""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (sinceTick: Tick) => ps.bind(Long box sinceTick)
  }

  private def where(name: String, count: Int): String = {
    if (count == 1) {
      s"$name = ?"
    } else {
      Seq.fill(count)("?").mkString(s"$name IN (", ",", ")")
    }
  }

  private val ReplayByChannels: Set[Channel] => BoundStatement = {
    val getStatement = new Memoizer((channelCount: Int) => {
      val channelMatch = where("channel", channelCount)
      session.prepare(s"""
        SELECT $txColumns
        FROM $TableName
        WHERE $channelMatch
        ALLOW FILTERING
        """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    })
    (channels: Set[Channel]) => {
      val ps = getStatement(channels.size)
      val args = channels.toSeq
      ps.bind(args: _*)
    }
  }

  private val ReplayByChannelsSince: (Set[Channel], Long) => BoundStatement = {
    val getStatement = new Memoizer((channelCount: Int) => {
      val channelMatch = where("channel", channelCount)
      session.prepare(s"""
        SELECT $txColumns
        FROM $TableName
        WHERE $channelMatch
        AND tick >= ?
        ALLOW FILTERING
        """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    })
    (channels: Set[Channel], sinceTick: Tick) => {
      val ps = getStatement(channels.size)
      val args: List[Object] = channels.toList :+ Long.box(sinceTick)
      ps.bind(args: _*)
    }
  }
  private def ReplayByEvent: (Channel, Class[_ <: EVT]) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txColumns
      FROM $TableName
      WHERE channel = ?
      AND event_names CONTAINS ?
      ALLOW FILTERING
      """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    (channel: Channel, evtType: Class[_ <: EVT]) => {
      val evtName = evtFmt.signature(evtType).name
      ps.bind(channel, evtName)
    }
  }
  private def ReplayByEventSince: (Channel, Class[_ <: EVT], Long) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txColumns
      FROM $TableName
      WHERE channel = ?
      AND event_names CONTAINS ?
      AND tick >= ?
      ALLOW FILTERING
      """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    (channel: Channel, evtType: Class[_ <: EVT], sinceTick: Tick) => {
      val evtName = evtFmt.signature(evtType).name
      ps.bind(channel, evtName, Long box sinceTick)
    }
  }

  private lazy val ReplayStream: ID => BoundStatement = {
    val ps = session.prepare(s"SELECT $streamColumns FROM $TableName WHERE stream_id = ?")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID) => ps.bind(ct[ID].writeAs(id))
  }
  def replayStream[R](stream: ID)(callback: StreamConsumer[Transaction, R]): Unit = {
    val stm = ReplayStream(stream)
    queryAsync(Some(stream), callback, stm)
  }

  private lazy val ReplayStreamFrom: (ID, Int) => BoundStatement = {
    val ps = session.prepare(s"SELECT $streamColumns FROM $TableName WHERE stream_id = ? AND revision >= ?")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, fromRev: Revision) => ps.bind(ct[ID].writeAs(id), Int.box(fromRev))
  }
  def replayStreamFrom[R](stream: ID, fromRevision: Revision)(callback: StreamConsumer[Transaction, R]): Unit = {
    if (fromRevision == 0) {
      replayStream(stream)(callback)
    } else {
      val stm = ReplayStreamFrom(stream, fromRevision)
      queryAsync(Some(stream), callback, stm)
    }
  }

  private lazy val ReplayStreamTo: (ID, Int) => BoundStatement = {
    val ps = session.prepare(s"SELECT $streamColumns FROM $TableName WHERE stream_id = ? AND revision <= ?")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, toRev: Revision) => ps.bind(ct[ID].writeAs(id), Int.box(toRev))
  }
  override def replayStreamTo[R](stream: ID, toRevision: Revision)(callback: StreamConsumer[Transaction, R]): Unit = {
    val stm = ReplayStreamTo(stream, toRevision)
    queryAsync(Some(stream), callback, stm)
  }

  private lazy val ReplayStreamRange: (ID, Range) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $streamColumns
      FROM $TableName
      WHERE stream_id = ?
      AND revision >= ? AND revision <= ?
      """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, range: Range) => {
      val first = Int box range.head
      val last = Int box range.last
      assert(first < last)
      ps.bind(ct[ID].writeAs(id), first, last)
    }
  }
  def replayStreamRange[R](stream: ID, revisionRange: Range)(callback: StreamConsumer[Transaction, R]): Unit = {
    require(revisionRange.step == 1, s"Revision range must step by 1 only, not ${revisionRange.step}")
    val from = revisionRange.head
    val to = revisionRange.last
    if (from == to) {
      replayStreamRevision(stream, from)(callback)
    } else if (from == 0) {
      replayStreamTo(stream, to)(callback)
    } else {
      val ps = ReplayStreamRange(stream, revisionRange)
      queryAsync(Some(stream), callback, ps)
    }
  }

  private lazy val ReplayStreamRevision: (ID, Int) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $streamColumns
      FROM $TableName
      WHERE stream_id = ?
      AND revision = ?""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (stream: ID, rev: Revision) => ps.bind(ct[ID].writeAs(stream), Int box rev)
  }
  private def replayStreamRevision[U](stream: ID, revision: Revision)(callback: StreamConsumer[Transaction, U]): Unit = {
    val stm = ReplayStreamRevision(stream, revision)
    queryAsync(Some(stream), callback, stm)
  }

  private lazy val RecordFirstRevision = {
    val ps = session.prepare(s"""
      INSERT INTO $TableName
      (stream_id, tick, event_names, event_versions, event_data, metadata, channel, revision)
      VALUES(?,?,?,?,?,?,?,0) IF NOT EXISTS""").setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, channel: Channel, tick: Tick, events: List[EVT], metadata: Map[String, String]) => {
      val (jTypes, jVers, jData) = toJLists(events)
      ps.bind(
        ct[ID].writeAs(id),
        Long box tick,
        jTypes, jVers, jData,
        metadata.asJava,
        channel)
    }
  }
  private lazy val RecordLaterRevision = {
    val ps = session.prepare(s"""
      INSERT INTO $TableName
      (stream_id, tick, event_names, event_versions, event_data, metadata, revision)
      VALUES(?,?,?,?,?,?,?) IF NOT EXISTS""").setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, revision: Revision, tick: Tick, events: List[EVT], metadata: Map[String, String]) => {
      val (jTypes, jVers, jData) = toJLists(events)
      ps.bind(
        ct[ID].writeAs(id),
        Long box tick,
        jTypes, jVers, jData,
        metadata.asJava,
        Int box revision)
    }
  }
  protected def insert(
      channel: Channel, stream: ID, revision: Revision, tick: Tick,
      events: List[EVT], metadata: Map[String, String])(
      handler: ResultSet => Transaction): Future[Transaction] = {
    if (revision == 0) {
      val stm = RecordFirstRevision(stream, channel, tick, events, metadata)
      execute(stm)(handler)
    } else if (revision > 0) {
      val stm = RecordLaterRevision(stream, revision, tick, events, metadata)
      execute(stm)(handler)
    } else {
      throw new IllegalArgumentException(s"Cannot record negative revision: $revision")
    }
  }

  def commit(
      channel: Channel, stream: ID, revision: Revision, tick: Tick,
      events: List[EVT], metadata: Map[String, String]): Future[Transaction] = {
    insert(channel, stream, revision, tick, events, metadata) { rs =>
      if (rs.wasApplied) {
        Transaction(tick, channel, stream, revision, metadata, events)
      } else {
        val conflicting = toTransaction(Some(stream), rs.one, Columns(rs.getColumnDefinitions.getIndexOf))
        throw new DuplicateRevisionException(conflicting)
      }
    }

  }

  def query[U](selector: Selector)(callback: StreamConsumer[Transaction, U]): Unit = {
    resolve(selector, None) match {
      case Right(stms) =>
        processMultiple(callback.onNext, stms).onComplete {
          case Success(_) => callback.onDone()
          case Failure(NonFatal(th)) => callback.onError(th)
        }
      case Left((id, stm)) =>
        queryAsync(Some(id), callback, stm)
    }
  }

  def querySince[U](sinceTick: Tick, selector: Selector)(callback: StreamConsumer[Transaction, U]): Unit = {
    resolve(selector, Some(sinceTick)) match {
      case Right(stms) =>
        processMultiple(callback.onNext, stms).onComplete {
          case Success(_) => callback.onDone()
          case Failure(NonFatal(th)) => callback.onError(th)
        }
      case Left((id, stm)) =>
        queryAsync(Some(id), callback, stm)
    }
  }

  private def resolve(selector: Selector, sinceTick: Option[Tick]): Either[(ID, BoundStatement), Seq[BoundStatement]] = {

    selector match {
      case Everything => Right {
        sinceTick match {
          case Some(sinceTick) => ReplayEverythingSince(sinceTick) :: Nil
          case None => ReplayEverything() :: Nil
        }
      }
      case ChannelSelector(channels) => Right {
        sinceTick match {
          case Some(sinceTick) => ReplayByChannelsSince(channels, sinceTick) :: Nil
          case None => ReplayByChannels(channels) :: Nil
        }
      }
      case EventSelector(byChannel) => Right {
        val replayByEvent = sinceTick match {
          case Some(sinceTick) => ReplayByEventSince(_: Channel, _: Class[_ <: EVT], sinceTick)
          case None => ReplayByEvent
        }
        byChannel.toList.flatMap {
          case (ch, evtTypes) =>
            evtTypes.toSeq.map(replayByEvent curried ch)
        }
      }
      case SingleStreamSelector(id, _) => Left(id -> ReplayStream(id))
    }
  }

}

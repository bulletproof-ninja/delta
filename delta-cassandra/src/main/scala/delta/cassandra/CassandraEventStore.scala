package delta.cassandra

import java.util.{ ArrayList, List => JList }

import scala.{ Left, Right }
//import scala.collection.{ Map => aMap, Seq => aSeq }
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import com.datastax.driver.core._

import scuff.Memoizer
import scuff.concurrent.{ StreamCallback, exeCtxToExecutor }
import delta.{ EventCodec, EventStore }

private[cassandra] object CassandraEventStore {

  private def ensureTable[ID: ColumnType, CH: ColumnType, SF: ColumnType](
    session: Session, keyspace: String, table: String, replication: Map[String, Any]) {
    val replicationStr = replication.map {
      case (key, str: CharSequence) => s"'$key':'$str'"
      case (key, cls: Class[_]) => s"'$key':'${cls.getName}'"
      case (key, any) => s"'$key':$any"
    }.mkString("{", ",", "}")
      def cqlName[T: ColumnType]: String = implicitly[ColumnType[ID]].typeName
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStr")
    session.execute(s"""
      CREATE TABLE IF NOT EXISTS $keyspace.$table (
        stream_id ${cqlName[ID]},
        revision INT,
        tick BIGINT,
        channel ${cqlName[CH]} STATIC,
        event_names LIST<TEXT>,
        event_versions LIST<TINYINT>,
        event_data LIST<${cqlName[SF]}>,
        metadata MAP<TEXT,TEXT>,
        PRIMARY KEY ((stream_id), revision)
      ) WITH CLUSTERING ORDER BY (revision ASC)""")
    session.execute(s"""CREATE INDEX IF NOT EXISTS ON $keyspace.$table(channel)""")
  }

  private val StreamColumns = Seq("revision", "tick", "channel", "event_names", "event_versions", "event_data", "metadata")
  private val TxnColumns = "stream_id" +: StreamColumns
  private def streamColumns: String = StreamColumns.mkString(",")
  private def txnColumns: String = TxnColumns.mkString(",")
  private val StreamColumnsIdx = Columns(StreamColumns.indexOf)
  private val TxnColumnsIdx = Columns(TxnColumns.indexOf)

  private case class Columns(
    stream_id: Int, revision: Int,
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
abstract class CassandraEventStore[ID: ColumnType, EVT, CH: ColumnType, SF: ColumnType](
  session: Session,
  td: TableDescriptor)(implicit exeCtx: ExecutionContext, codec: EventCodec[EVT, SF])
    extends EventStore[ID, EVT, CH] {

  import CassandraEventStore._

  ensureTable[ID, CH, SF](session, td.keyspace, td.table, td.replication)

  private def ct[T: ColumnType] = implicitly[ColumnType[T]]

  private[this] val TableName = s"${td.keyspace}.${td.table}"

  protected def toTransaction(knownStream: Option[ID], row: Row, columns: Columns): TXN = {
    val stream = knownStream getOrElse ct[ID].readFrom(row, columns.stream_id)
    val tick = row.getLong(columns.tick)
    val channel = ct[CH].readFrom(row, columns.channel)
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
    val eventVersions = row.getList(columns.event_versions, classOf[Byte])
    val eventData = row.getList(columns.event_data, ct[SF].jvmType)
    val events = fromJLists(eventNames, eventVersions, eventData)
    Transaction(tick, channel, stream, revision, metadata, events)
  }

  private def execute[T](stm: BoundStatement)(handler: ResultSet => T): Future[T] = {
    val result = session.executeAsync(stm)
    val promise = Promise[T]
    val listener = new Runnable {
      def run: Unit = promise complete Try(handler(result.get))
    }
    result.addListener(listener, exeCtx)
    promise.future
  }

//  private def queryStream(callback: StreamCallback[TXN], stm: PreparedStatement, stream: ID, parms: Any*) {
//    val streamParm = ct[ID].writeAs(stream)
//    val refParms = streamParm +: parms.map(_.asInstanceOf[AnyRef])
//    val bound = stm.bind(refParms: _*)
//    execute(bound) { rs =>
//      Try {
//        val iter = rs.iterator().asScala.map(row => toTransaction(Some(stream), row, StreamColumnsIdx))
//        while (iter.hasNext) callback.onNext(iter.next)
//      } match {
//        case Success(_) => callback.onCompleted()
//        case Failure(NonFatal(e)) => callback.onError(e)
//      }
//    }
//  }

  // TODO: Make fully callback driven:
  private def processMultiple(callback: TXN => Unit, stms: Seq[BoundStatement]): Future[Unit] = {
      def iterate(rss: Seq[ResultSet]): Seq[ResultSet] = {
        for {
          rs <- rss
          _ <- 0 until rs.getAvailableWithoutFetching
        } {
          callback(toTransaction(None, rs.one, TxnColumnsIdx))
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
  private def queryAsync(stream: Some[ID], callback: StreamCallback[TXN], stm: BoundStatement) {
    execute(stm) { rs =>
      Try {
        val iter = rs.iterator().asScala.map(row => toTransaction(stream, row, StreamColumnsIdx))
        while (iter.hasNext) callback.onNext(iter.next)
      } match {
        case Success(_) => callback.onCompleted()
        case Failure(NonFatal(e)) => callback.onError(e)
      }
    }
  }

  //  protected def execute[T](stm: PreparedStatement, parms: Any*)(handler: ResultSet => T): Future[T] = {
  //    val refParms = parms.map(_.asInstanceOf[AnyRef]).toSeq
  //    val bound = stm.bind(refParms: _*)
  //    execute(bound)(handler)
  //  }

  private def fromJLists(types: JList[String], vers: JList[Byte], data: JList[SF]): List[EVT] = {
    val size = types.size
    assert(vers.size == size && data.size == size)
    var idx = size - 1
    var list = List.empty[EVT]
    while (idx != -1) {
      list = codec.decode(types.get(idx), vers.get(idx), data.get(idx)) :: list
      idx -= 1
    }
    list
  }
  private def toJLists(events: List[EVT]): (JList[String], JList[Int], JList[SF]) = {
    val types = new ArrayList[String](8)
    val typeVers = new ArrayList[Int](8)
    val data = new ArrayList[SF](8)
    events.foreach { evt =>
      types add codec.name(evt)
      typeVers add codec.version(evt)
      data add codec.encode(evt)
    }
    (types, typeVers, data)
  }

  private val CurrRevision = {
    val ps = session.prepare(s"""
      SELECT revision
      FROM $TableName
      WHERE stream_id = ?
      ORDER BY revision DESC
      LIMIT 1""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID) => ps.bind(ct[ID].writeAs(id))
  }
  def currRevision(streamId: ID): Future[Option[Int]] = {
    execute(CurrRevision(streamId))(rs => Option(rs.one).map(_.getInt(0)))
  }

  //  private val GetTickRange = {
  //    val ps = session.prepare(s"SELECT MIN(tick), MAX(tick) FROM $TableName")
  //    () => ps.bind()
  //  }
  //  private def tickRange(): Future[Option[TickRange]] = {
  //    execute(GetTickRange()) { rs =>
  //      Option(rs.one).map { row =>
  //        TickRange(row.getLong(0), row.getLong(1))
  //      }
  //    }
  //  }
  private val GetLastTick = {
    val ps = session.prepare(s"SELECT MAX(tick) FROM $TableName")
    () => ps.bind()
  }
  def maxTickCommitted(): Future[Option[Long]] =
    execute(GetLastTick()) { rs =>
      Option(rs.one).map(_.getLong(0))
    }

  private val ReplayEverything: () => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txnColumns
      FROM $TableName
      """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    () => ps.bind()
  }
  private val ReplayEverythingSince: Long => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txnColumns
      FROM $TableName
      WHERE tick >= ?
      ALLOW FILTERING""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (sinceTick: Long) => ps.bind(Long.box(sinceTick))
  }

  private def where(name: String, count: Int): String = {
    if (count == 1) {
      s"$name = ?"
    } else {
      Seq.fill(count)("?").mkString(s"$name IN (", ",", ")")
    }
  }

  private val ReplayByChannels: Set[CH] => BoundStatement = {
    val getStatement = new Memoizer((channelCount: Int) => {
      val channelMatch = where("channel", channelCount)
      session.prepare(s"""
        SELECT $txnColumns
        FROM $TableName
        WHERE $channelMatch
        ALLOW FILTERING
        """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    })
    (channels: Set[CH]) => {
      val ps = getStatement(channels.size)
      val args = channels.toSeq.map(ct[CH].writeAs)
      ps.bind(args: _*)
    }
  }
  private val ReplayByChannelsSince: (Set[CH], Long) => BoundStatement = {
    val getStatement = new Memoizer((channelCount: Int) => {
      val channelMatch = where("channel", channelCount)
      session.prepare(s"""
        SELECT $txnColumns
        FROM $TableName
        WHERE $channelMatch
        AND tick >= ?
        ALLOW FILTERING
        """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    })
    (channels: Set[CH], sinceTick: Long) => {
      val ps = getStatement(channels.size)
      val args = channels.toSeq.map(ct[CH].writeAs) :+ Long.box(sinceTick)
      ps.bind(args: _*)
    }
  }
  private def ReplayByEvent: (CH, Class[_ <: EVT]) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txnColumns
      FROM $TableName
      WHERE channel = ?
      AND event_names CONTAINS ?
      ALLOW FILTERING
      """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    (ch: CH, evtType: Class[_ <: EVT]) => {
      val channel = ct[CH].writeAs(ch)
      val evtName = codec.name(evtType)
      ps.bind(channel, evtName)
    }
  }
  private def ReplayByEventSince: (CH, Class[_ <: EVT], Long) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txnColumns
      FROM $TableName
      WHERE channel = ?
      AND event_names CONTAINS ?
      AND tick >= ?
      ALLOW FILTERING
      """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    (ch: CH, evtType: Class[_ <: EVT], sinceTick: Long) => {
      val channel = ct[CH].writeAs(ch)
      val evtName = codec.name(evtType)
      ps.bind(channel, evtName, Long box sinceTick)
    }
  }

  private val ReplayStream: ID => BoundStatement = {
    val ps = session.prepare(s"SELECT $streamColumns FROM $TableName WHERE stream_id = ? ORDER BY revision")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID) => ps.bind(ct[ID].writeAs(id))
  }
  def replayStream(stream: ID)(callback: StreamCallback[TXN]): Unit = {
    val stm = ReplayStream(stream)
    queryAsync(Some(stream), callback, stm)
  }

  private val ReplayStreamFrom: (ID, Int) => BoundStatement = {
    val ps = session.prepare(s"SELECT $streamColumns FROM $TableName WHERE stream_id = ? AND revision >= ? ORDER BY revision")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, fromRev: Int) => ps.bind(ct[ID].writeAs(id), Int.box(fromRev))
  }
  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[TXN]): Unit = {
    if (fromRevision == 0) {
      replayStream(stream)(callback)
    } else {
      val stm = ReplayStreamFrom(stream, fromRevision)
      queryAsync(Some(stream), callback, stm)
    }
  }

  private val ReplayStreamTo: (ID, Int) => BoundStatement = {
    val ps = session.prepare(s"SELECT $streamColumns FROM $TableName WHERE stream_id = ? AND revision <= ? ORDER BY revision")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, toRev: Int) => ps.bind(ct[ID].writeAs(id), Int.box(toRev))
  }
  override def replayStreamTo(stream: ID, toRevision: Int)(callback: StreamCallback[TXN]): Unit = {
    val stm = ReplayStreamTo(stream, toRevision)
    queryAsync(Some(stream), callback, stm)
  }

  private val ReplayStreamRange: (ID, Range) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $streamColumns
      FROM $TableName
      WHERE stream_id = ?
      AND revision >= ? AND revision <= ?
      ORDER BY revision
      """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, range: Range) => {
      val first = Int box range.head
      val last = Int box range.last
      assert(first < last)
      ps.bind(ct[ID].writeAs(id), first, last)
    }
  }
  def replayStreamRange(stream: ID, revisionRange: Range)(callback: StreamCallback[TXN]): Unit = {
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

  private val ReplayStreamRevision: (ID, Int) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $streamColumns
      FROM $TableName
      WHERE stream_id = ?
      AND revision = ?""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (stream: ID, rev: Int) => ps.bind(ct[ID].writeAs(stream), Int box rev)
  }
  private def replayStreamRevision(stream: ID, revision: Int)(callback: StreamCallback[TXN]): Unit = {
    val stm = ReplayStreamRevision(stream, revision)
    queryAsync(Some(stream), callback, stm)
  }

  private val RecordFirstRevision = {
    val ps = session.prepare(s"""
      INSERT INTO $TableName
      (stream_id, tick, event_name, event_versions, event_data, metadata, channel, revision)
      VALUES(?,?,?,?,?,?,?,0) IF NOT EXISTS""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, channel: CH, tick: Long, events: List[EVT], metadata: Map[String, String]) => {
      val (jTypes, jVers, jData) = toJLists(events)
      ps.bind(
        ct[ID].writeAs(id),
        Long box tick,
        jTypes, jVers, jData,
        metadata.asJava,
        ct[CH].writeAs(channel))
    }
  }
  private val RecordLaterRevision = {
    val ps = session.prepare(s"""
      INSERT INTO $TableName
      (stream_id, tick, event_name, event_versions, event_data, metadata, revision)
      VALUES(?,?,?,?,?,?,?) IF NOT EXISTS""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, revision: Int, tick: Long, events: List[EVT], metadata: Map[String, String]) => {
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
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: List[EVT], metadata: Map[String, String])(
      handler: ResultSet => TXN): Future[TXN] = {
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
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: List[EVT], metadata: Map[String, String]): Future[TXN] = {
    insert(channel, stream, revision, tick, events, metadata) { rs =>
      if (rs.wasApplied) {
        Transaction(tick, channel, stream, revision, metadata, events)
      } else {
        val conflicting = toTransaction(Some(stream), rs.one, Columns(rs.getColumnDefinitions.getIndexOf))
        throw new DuplicateRevisionException(conflicting)
      }
    }

  }

  def query(selector: Selector)(callback: StreamCallback[TXN]): Unit = {
    resolve(selector, None) match {
      case Right(stms) =>
        processMultiple(callback.onNext, stms).onComplete {
          case Success(_) => callback.onCompleted()
          case Failure(NonFatal(th)) => callback.onError(th)
        }(exeCtx)
      case Left((id, stm)) =>
        queryAsync(Some(id), callback, stm)
    }
  }

  def querySince(sinceTick: Long, selector: Selector)(callback: StreamCallback[TXN]): Unit = {
    resolve(selector, Some(sinceTick)) match {
      case Right(stms) =>
        processMultiple(callback.onNext, stms).onComplete {
          case Success(_) => callback.onCompleted()
          case Failure(NonFatal(th)) => callback.onError(th)
        }(exeCtx)
      case Left((id, stm)) =>
        queryAsync(Some(id), callback, stm)
    }
  }

  private def resolve(selector: Selector, sinceTick: Option[Long]): Either[(ID, BoundStatement), Seq[BoundStatement]] = {

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
          case Some(sinceTick) => ReplayByEventSince(_: CH, _: Class[_ <: EVT], sinceTick)
          case None => ReplayByEvent
        }
        byChannel.toList.flatMap {
          case (ch, evtTypes) =>
            evtTypes.toSeq.map(replayByEvent curried ch)
        }
      }
      case StreamSelector(id, _) => Left(id -> ReplayStream(id))
    }
  }

}

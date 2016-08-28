package ulysses.cassandra

import ulysses._
import com.datastax.driver.core._
import scala.reflect.{ ClassTag, classTag }
import java.util.{ UUID, List => JList, Map => JMap, ArrayList }
import scuff.concurrent._
import scala.concurrent._
import scala.util.{ Try, Success, Failure }
import collection.JavaConverters._
import scuff.Memoizer
import collection.{ Seq => aSeq, Map => aMap }
import collection.immutable.Seq
import scuff.Codec
import ulysses.EventContext
import scala.util.control.NonFatal
import scala.collection.immutable.NumericRange

private[cassandra] object CassandraEventStore {

  //  val CassandraTypes: Map[Class[_], String] = Map(
  //    classOf[Array[Byte]] -> "BLOB",
  //    classOf[UUID] -> "UUID",
  //    classOf[String] -> "TEXT",
  //    classOf[Long] -> "BIGINT",
  //    classOf[java.lang.Long] -> "BIGINT",
  //    classOf[Int] -> "INT",
  //    classOf[java.lang.Integer] -> "INT",
  //    classOf[BigInt] -> "VARINT",
  //    classOf[java.math.BigInteger] -> "VARINT")

  private def ensureTable[ID: TypeConverter, CH: TypeConverter, SF: TypeConverter](
    session: Session, keyspace: String, table: String, replication: Map[String, Any]) {
    val replicationStr = replication.map {
      case (key, str: CharSequence) => s"'$key':'$str'"
      case (key, any) => s"'$key':$any"
    }.mkString("{", ",", "}")
      def cqlName[T: TypeConverter]: String = implicitly[TypeConverter[ID]].typeName
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStr")
    session.execute(s"""CREATE TABLE IF NOT EXISTS $keyspace.$table (
        stream ${cqlName[ID]},
        revision INT,
        tick BIGINT,
        channel ${cqlName[CH]} STATIC,
        eventType LIST<TEXT>,
        eventTypeVer LIST<SMALLINT>,
        eventData LIST<${cqlName[SF]}>,
        metadata MAP<TEXT,TEXT>,
        PRIMARY KEY ((stream), revision)
    ) WITH CLUSTERING ORDER BY (revision ASC);""")
  }

  private val StreamColumns = Seq("revision", "tick", "channel", "eventType", "eventTypeVer", "eventData", "metadata")
  private val TxnColumns = "stream" +: StreamColumns
  private def streamColumns: String = StreamColumns.mkString(",")
  private def txnColumns: String = TxnColumns.mkString(",")
  private val StreamColumnsIdx = Columns(StreamColumns.indexOf)
  private val TxnColumnsIdx = Columns(TxnColumns.indexOf)

  private case class Columns(
    stream: Int, revision: Int,
    tick: Int, channel: Int,
    eventType: Int, eventTypeVer: Int, eventData: Int,
    metadata: Int)
  private object Columns {
    def apply(colIdx: String => Int): Columns = {
      new Columns(
        stream = colIdx("stream"),
        revision = colIdx("revision"),
        tick = colIdx("tick"),
        channel = colIdx("channel"),
        eventType = colIdx("eventType"),
        eventTypeVer = colIdx("eventTypeVer"),
        eventData = colIdx("eventData"),
        metadata = colIdx("metadata"))
    }
    //    def apply(colDef: ColumnDefinitions): Columns = {
    //      new Columns(
    //        stream = colDef.getIndexOf("stream"),
    //        revision = colDef.getIndexOf("revision"),
    //        tick = colDef.getIndexOf("tick"),
    //        channel = colDef.getIndexOf("channel"),
    //        eventType = colDef.getIndexOf("eventType"),
    //        eventTypeVer = colDef.getIndexOf("eventTypeVer"),
    //        eventData = colDef.getIndexOf("eventData"),
    //        metadata = colDef.getIndexOf("metadata"))
    //    }
  }
  private case class TickRange(first: Long, last: Long) {
    def asArgs = Seq(boxFirst, boxLast)
    def boxFirst = Long box first
    def boxLast = Long box last
    val tickCount = last + 1 - first
    def inRange(tick: Long) = tick >= first && tick <= last
  }
}

trait TableDescriptor {
  def keyspace: String
  def table: String
  def replication: Map[String, Any]
}

/**
  * Cassandra event store implementation.
  * WARNING: Not tested WHAT SO EVER.
  * @param session The Cassandra session (connection pool)
  * @param td The table descriptor
  * @param tickChunkSize The tick range to include when doing a global query.
  * The size should be a function of how many transactions are expected in
  * each chunk, as those will be held in memory and sorted.
  */
class CassandraEventStore[ID: TypeConverter, EVT, CH: TypeConverter, SF: TypeConverter](
  session: Session,
  td: TableDescriptor,
  tickChunkSize: Int)(implicit exeCtx: ExecutionContext, evtCtx: EventContext[EVT, CH, SF])
    extends EventStore[ID, EVT, CH] {

  import CassandraEventStore._

  ensureTable[ID, CH, SF](session, td.keyspace, td.table, td.replication)

  private def tc[T: TypeConverter] = implicitly[TypeConverter[T]]

  private[this] val TableName = s"${td.keyspace}.${td.table}"

  protected def toTransaction(knownStream: Option[ID], row: Row, columns: Columns): TXN = {
    val stream = knownStream getOrElse tc[ID].readFrom(row, columns.stream)
    val tick = row.getLong(columns.tick)
    val channel = tc[CH].readFrom(row, columns.channel)
    val revision = row.getInt(columns.revision)
    val metadata = {
      val map = row.getMap(columns.metadata, classOf[String], classOf[String])
      if (map.isEmpty) {
        Map.empty[String, String]
      } else {
        map.asScala.toMap
      }
    }
    val eventType = row.getList(columns.eventType, classOf[String])
    val typeVers = row.getList(columns.eventTypeVer, classOf[Short])
    val eventData = row.getList(columns.eventData, tc[SF].jvmType)
    val events = fromJLists(eventType, typeVers, eventData)
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

  private def queryStream(callback: StreamCallback[TXN], stm: PreparedStatement, stream: ID, parms: Any*) {
    val streamParm = tc[ID].writeAs(stream)
    val refParms = streamParm +: parms.map(_.asInstanceOf[AnyRef])
    val bound = stm.bind(refParms: _*)
    execute(bound) { rs =>
      Try {
        val iter = rs.iterator().asScala.map(row => toTransaction(Some(stream), row, StreamColumnsIdx))
        while (iter.hasNext) callback.onNext(iter.next)
      } match {
        case Success(_) => callback.onCompleted()
        case Failure(NonFatal(e)) => callback.onError(e)
      }
    }
  }
  private def queryAsync(stream: Option[ID], callback: StreamCallback[TXN], stm: BoundStatement) {
    //    val streamParm = tc[ID].writeAs(stream)
    //    val refParms = streamParm +: parms.map(_.asInstanceOf[AnyRef])
    //    val bound = stm.bind(refParms: _*)
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

  private def fromJLists(types: JList[String], vers: JList[Short], data: JList[SF]): Seq[EVT] = {
    types.iterator.asScala.zip(vers.iterator.asScala).zip(data.iterator.asScala).foldLeft(Vector.empty[EVT]) {
      case (events, ((evtName, version), data)) =>
        events :+ evtCtx.decode(evtName, version, data)
    }
  }
  private def toJLists(events: aSeq[EVT]): (JList[String], JList[Int], JList[SF]) = {
    val types = new ArrayList[String](events.size)
    val typeVers = new ArrayList[Int](events.size)
    val data = new ArrayList[SF](events.size)
    events.foreach { evt =>
      types add evtCtx.name(evt)
      typeVers add evtCtx.version(evt)
      data add evtCtx.encode(evt)
    }
    (types, typeVers, data)
  }

  private val CurrRevision = {
    val ps = session.prepare(s"""
      SELECT revision
      FROM $TableName
      WHERE stream = ?
      ORDER BY revision DESC
      LIMIT 1""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID) => ps.bind(tc[ID].writeAs(id))
  }
  def currRevision(streamId: ID): Future[Option[Int]] = {
    execute(CurrRevision(streamId))(rs => Option(rs.one).map(_.getInt(0)))
  }

  private val GetTickRange = {
    val ps = session.prepare(s"SELECT MIN(tick), MAX(tick) FROM $TableName")
    () => ps.bind()
  }
  private def tickRange(): Future[Option[TickRange]] = {
    execute(GetTickRange()) { rs =>
      Option(rs.one).map { row =>
        TickRange(row.getLong(0), row.getLong(1))
      }
    }
  }
  private val GetLastTick = {
    val ps = session.prepare(s"SELECT MAX(tick) FROM $TableName")
    () => ps.bind()
  }
  def lastTick: Future[Option[Long]] =
    execute(GetLastTick()) { rs =>
      Option(rs.one).map(_.getLong(0))
    }

  private val ReplayEverything: (TickRange) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txnColumns
      FROM $TableName
      WHERE tick >= ? AND tick <= ?
      ALLOW FILTERING""")
    (tickRange: TickRange) => ps.bind(tickRange.asArgs: _*)
  }
  private val ReplayByChannels: (Set[CH], TickRange) => BoundStatement = {
    val getStatement = new Memoizer((channelCount: Int) => {
      val channelMatch = if (channelCount == 1) {
        "channel = ?"
      } else {
        Seq.fill(channelCount)("?").mkString(s"channel IN (", ",", ")")
      }
      session.prepare(s"""
        SELECT $txnColumns
        FROM $TableName
        WHERE $channelMatch
        AND tick >= ? AND tick <= ?
        ALLOW FILTERING
        """)
    })
    (channels: Set[CH], tickRange: TickRange) => {
      val ps = getStatement(channels.size)
      val args = channels.toSeq.map(tc[CH].writeAs) ++ tickRange.asArgs
      ps.bind(args: _*)
    }
  }
  private def ReplayByEvent: (Class[_ <: EVT], TickRange) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $txnColumns
      FROM $TableName
      WHERE channel = ?
      AND evtTypes CONTAINS ?
      AND tick >= ? AND tick <= ?
      ALLOW FILTERING
      """)
    (evtType: Class[_ <: EVT], tickRange: TickRange) => {
      val channel = tc[CH].writeAs(evtCtx.channel(evtType))
      val evtName = evtCtx.name(evtType)
      ps.bind(channel, evtName, tickRange.boxFirst, tickRange.boxLast)
    }
  }

  private val ReplayStream: ID => BoundStatement = {
    val ps = session.prepare(s"SELECT $streamColumns FROM $TableName WHERE stream = ? ORDER BY revision")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID) => ps.bind(tc[ID].writeAs(id))
  }
  def replayStream(stream: ID)(callback: StreamCallback[TXN]): Unit = {
    val stm = ReplayStream(stream)
    queryAsync(Some(stream), callback, stm)
  }

  private val ReplayStreamFrom: (ID, Int) => BoundStatement = {
    val ps = session.prepare(s"SELECT $streamColumns FROM $TableName WHERE stream = ? AND revision >= ? ORDER BY revision")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, fromRev: Int) => ps.bind(tc[ID].writeAs(id), Int.box(fromRev))
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
    val ps = session.prepare(s"SELECT $streamColumns FROM $TableName WHERE stream = ? AND revision <= ? ORDER BY revision")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, toRev: Int) => ps.bind(tc[ID].writeAs(id), Int.box(toRev))
  }
  override def replayStreamTo(stream: ID, toRevision: Int)(callback: StreamCallback[TXN]): Unit = {
    val stm = ReplayStreamTo(stream, toRevision)
    queryAsync(Some(stream), callback, stm)
  }

  private val ReplayStreamRange: (ID, Range) => BoundStatement = {
    val ps = session.prepare(s"""
      SELECT $streamColumns
      FROM $TableName
      WHERE stream = ?
      AND revision >= ? AND revision <= ?
      ORDER BY revision
      """).setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, range: Range) => {
      val first = Int box range.head
      val last = Int box range.last
      assert(first < last)
      ps.bind(tc[ID].writeAs(id), first, last)
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
      WHERE stream = ?
      AND revision = ?""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (stream: ID, rev: Int) => ps.bind(tc[ID].writeAs(stream), Int box rev)
  }
  private def replayStreamRevision(stream: ID, revision: Int)(callback: StreamCallback[TXN]): Unit = {
    val stm = ReplayStreamRevision(stream, revision)
    queryAsync(Some(stream), callback, stm)
  }

  private val RecordFirstRevision = {
    val ps = session.prepare(s"""
      INSERT INTO $TableName
      (stream, tick, eventType, eventTypeVer, eventData, metadata, channel, revision)
      VALUES(?,?,?,?,?,?,?,0) IF NOT EXISTS""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, channel: CH, tick: Long, events: aSeq[EVT], metadata: aMap[String, String]) => {
      val (jTypes, jVers, jData) = toJLists(events)
      ps.bind(
        tc[ID].writeAs(id),
        Long box tick,
        jTypes, jVers, jData,
        metadata.asJava,
        tc[CH].writeAs(channel))
    }
  }
  private val RecordLaterRevision = {
    val ps = session.prepare(s"""
      INSERT INTO $TableName
      (stream, tick, eventType, eventTypeVer, eventData, metadata, revision)
      VALUES(?,?,?,?,?,?,?) IF NOT EXISTS""").setConsistencyLevel(ConsistencyLevel.SERIAL)
    (id: ID, revision: Int, tick: Long, events: aSeq[EVT], metadata: aMap[String, String]) => {
      val (jTypes, jVers, jData) = toJLists(events)
      ps.bind(
        tc[ID].writeAs(id),
        Long box tick,
        jTypes, jVers, jData,
        metadata.asJava,
        Int box revision)
    }
  }
  protected def insert(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: aSeq[EVT], metadata: aMap[String, String])(
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

  def record(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: aSeq[EVT], metadata: aMap[String, String]): Future[TXN] = {

    insert(channel, stream, revision, tick, events, metadata) { rs =>
      if (rs.wasApplied) {
        Transaction(tick, channel, stream, revision, metadata, events)
      } else {
        val conflicting = toTransaction(Some(stream), rs.one, Columns(rs.getColumnDefinitions.getIndexOf))
        throw new DuplicateRevisionException(conflicting)
      }
    }

  }

  def replay(filter: StreamFilter[ID, EVT, CH])(callback: StreamCallback[TXN]): Unit = {
    tickRange().onComplete {
      case Success(Some(range)) => try {
        replayFullRange(range, filter, callback)
      } catch {
        case NonFatal(th) => callback.onError(th)
      }
      case Success(None) => callback.onCompleted()
      case Failure(th) => callback.onError(th)
    }
  }

  def replaySince(sinceTick: Long, filter: StreamFilter[ID, EVT, CH])(callback: StreamCallback[TXN]): Unit = {
    lastTick().onComplete {
      case Success(Some(last)) => try {
        replayFullRange(TickRange(sinceTick, last), filter, callback)
      } catch {
        case NonFatal(th) => callback.onError(th)
      }
      case Success(None) => callback.onCompleted()
      case Failure(th) => callback.onError(th)
    }
  }

  private def replayFullRange(
    fullTickRange: TickRange,
    filter: StreamFilter[ID, EVT, CH],
    callback: StreamCallback[TXN]): Unit = {
    resolve(filter) match {
      case Left((id, stm)) => execute(stm) { rs =>
        try {
          val colDefs = Columns(rs.getColumnDefinitions.getIndexOf)
          rs.iterator.asScala
            .map(toTransaction(Some(id), _, colDefs))
            .filter(txn => fullTickRange.inRange(txn.tick))
            .foreach(callback.onNext)
        } catch {
          case NonFatal(th) => callback onError th
        }
      }
      case Right(statements) =>
        val ticks = fullTickRange.tickCount
        require(ticks > 0, s"Invalid tick range: $tickRange")
        val chunkCount =
          if (ticks <= tickChunkSize) 1
          else ticks / tickChunkSize + (if (ticks % tickChunkSize == 0) 0 else 1)
        val queryRanges = (0L until chunkCount).map { chunkIdx =>
          val from = (fullTickRange.first + chunkIdx * tickChunkSize)
          TickRange(from, from + tickChunkSize - 1)
        }
        replayRanges(queryRanges.toList, statements, callback)
    }
  }

  private def resolve(filter: StreamFilter[ID, EVT, CH]): Either[(ID, BoundStatement), aSeq[TickRange => BoundStatement]] = {
    import StreamFilter._
    filter match {
      case Everything() => Right(Seq(ReplayEverything))
      case ByChannel(channels) => Right(Seq(ReplayByChannels.curried(channels)))
      case ByEvent(evtTypes, _) => Right {
        evtTypes.toSeq.map(ReplayByEvent.curried)
      }
      case ByStream(id, _) => Left(id -> ReplayStream(id))
    }
  }

  private def replayRanges(
    tickChunks: List[TickRange],
    statements: aSeq[TickRange => BoundStatement],
    callback: StreamCallback[TXN]): Unit =
    tickChunks match {
      case Nil => callback.onCompleted()
      case currTickChunk :: remaining =>
        replayTickChunk(statements.map(_(currTickChunk)), callback).onComplete {
          case Success(_) => replayRanges(remaining, statements, callback)
          case Failure(th) => callback onError th
        }
    }
  private def replayTickChunk(
    statements: aSeq[BoundStatement],
    callback: StreamCallback[TXN]): Future[Unit] = {
    val futures = statements.map { stm =>
      execute(stm) { rs =>
        rs.iterator().asScala.foldLeft(new collection.mutable.ArrayBuffer[TXN](4096)) {
          case (buffer, row) =>
            buffer += toTransaction(None, row, TxnColumnsIdx)
            buffer
        }
      }
    }
    val sorted = Future.sequence(futures).map { buffers =>
      val count = buffers.map(_.size).sum
      val (array, _) = buffers.foldLeft(new Array[TXN](count) -> 0) {
        case ((array, idx), buffer) =>
          buffer.copyToArray(array, idx)
          array -> (idx + buffer.size)
      }
      array.sortBy(_.tick)
    }
    sorted.map { array =>
      array.foreach(callback.onNext)
    }
  }

}

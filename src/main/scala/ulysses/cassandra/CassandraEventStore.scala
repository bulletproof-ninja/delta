package ulysses.cassandra

import ulysses._
import com.datastax.driver.core._
import scala.reflect.{ ClassTag, classTag }
import java.util.{ UUID, List => JList, Map => JMap, ArrayList }
import scuff.concurrent._
import scala.concurrent._
import scala.util.{ Try, Success, Failure }
import collection.JavaConverters._
import scuff.Multiton
import collection.immutable.Seq
import scuff.Codec
import ulysses.util.EventCodec
import scala.util.control.NonFatal

private[cassandra] object CassandraEventStore {

  val CassandraTypes: Map[Class[_], String] = Map(
    classOf[Array[Byte]] -> "BLOB",
    classOf[UUID] -> "UUID",
    classOf[String] -> "TEXT",
    classOf[Long] -> "BIGINT",
    classOf[java.lang.Long] -> "BIGINT",
    classOf[Int] -> "INT",
    classOf[java.lang.Integer] -> "INT",
    classOf[BigInt] -> "VARINT",
    classOf[java.math.BigInteger] -> "VARINT")

  private def ensureTable[ID: TypeConverter, CAT: TypeConverter](
    session: Session, keyspace: String, table: String, replication: Map[String, Any]) {
    val replicationStr = replication.map {
      case (key, str: CharSequence) => s"'$key':'$str'"
      case (key, any) => s"'$key':$any"
    }.mkString("{", ",", "}")
      def typeClass[T: TypeConverter]: Class[_] = implicitly[TypeConverter[ID]].cassandraType.runtimeClass
    val idTypeStr = CassandraTypes.getOrElse(typeClass[ID], sys.error(s"Unsupported ID type: ${typeClass[ID].getName}"))
    val catTypeStr = CassandraTypes.getOrElse(typeClass[CAT], sys.error(s"Unsupported category type: ${typeClass[CAT].getName}"))
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStr")
    session.execute(s"""CREATE TABLE IF NOT EXISTS $keyspace.$table (
    		stream $idTypeStr,
    		rev INT,
    		time BIGINT,
    		category $catTypeStr STATIC,
        eventTypes LIST<TEXT>,
    		eventData LIST<TEXT>,
    		metadata MAP<TEXT,TEXT>,
    		PRIMARY KEY ((stream), rev)
    ) WITH CLUSTERING ORDER BY (rev ASC);""")
  }

  val StreamSpecificColumns = "rev, time, category, eventTypes, eventData, metadata"
  val FixedColumns = new Columns(0, 1, 2, 3, 4, 5)

  private case class Columns(revision: Int, time: Int, category: Int, eventTypes: Int, eventData: Int, metadata: Int)
  private object Columns {
    def apply(colDef: ColumnDefinitions): Columns = {
      new Columns(
        revision = colDef.getIndexOf("rev"),
        time = colDef.getIndexOf("time"),
        category = colDef.getIndexOf("category"),
        eventTypes = colDef.getIndexOf("eventTypes"),
        eventData = colDef.getIndexOf("eventData"),
        metadata = colDef.getIndexOf("metadata")
      )
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
  * WARNING: Not tested WHAT SO EVER.
  * Update: Is definitely not working.
  * TODO: Need to solve global query problem.
  */
class CassandraEventStore[ID: TypeConverter, EVT, CAT: TypeConverter](
  session: Session,
  td: TableDescriptor)(implicit exeCtx: ExecutionContext, evtCodec: EventCodec[EVT, String])
    extends EventStore[ID, EVT, CAT] {

  import CassandraEventStore._

  ensureTable[ID, CAT](session, td.keyspace, td.table, td.replication)

  protected final val TableName = s"${td.keyspace}.${td.table}"

  protected def toTransaction(stream: ID, row: Row, columns: Columns): Transaction = {
    val clock = row.getLong(columns.time)
    val category = implicitly[TypeConverter[CAT]].getValue(row, columns.category)
    val revision = row.getInt(columns.revision)
    val metadata = {
      val map = row.getMap(columns.metadata, classOf[String], classOf[String])
      if (map.isEmpty) {
        Map.empty[String, String]
      } else {
        map.asScala.toMap
      }
    }
    val events = fromStringList(row.getList(columns.eventTypes, classOf[String]), row.getList(columns.eventData, classOf[String]))
    new Transaction(clock, category, stream, revision, metadata, events)
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

  private def query(callback: StreamCallback[Transaction], stm: PreparedStatement, stream: ID, parms: Any*) {
    val streamParm = implicitly[TypeConverter[ID]].toCassandraType(stream).asInstanceOf[AnyRef]
    val refParms = streamParm +: parms.map(_.asInstanceOf[AnyRef]).toSeq
    val bound = stm.bind(refParms: _*)
    execute(bound) { rs =>
      Try {
        val iter = rs.iterator().asScala.map(row => toTransaction(stream, row, FixedColumns))
        while (iter.hasNext) callback.onNext(iter.next)
      } match {
        case Success(_) => callback.onCompleted()
        case Failure(NonFatal(e)) => callback.onError(e)
      }
    }
  }

  protected def execute[T](stm: PreparedStatement, parms: Any*)(handler: ResultSet => T): Future[T] = {
    val refParms = parms.map(_.asInstanceOf[AnyRef]).toSeq
    val bound = stm.bind(refParms: _*)
    execute(bound)(handler)
  }

  private def fromStringList(types: JList[String], data: JList[String]): Seq[EVT] = {
    types.iterator.asScala.zip(data.iterator.asScala).foldLeft(Vector.empty[EVT]) {
      case (events, (tv, data)) =>
        val sep = tv.lastIndexOf(':')
        val version = scuff.Numbers.parseUnsafeInt(tv, sep + 1).toShort
        val name = tv.substring(0, sep)
        events :+ evtCodec.decodeEvent(name, version, data)
    }
  }
  private def toStringLists(events: Seq[EVT]): (ArrayList[String], ArrayList[String]) = {
    val types = new ArrayList[String](events.size)
    val data = new ArrayList[String](events.size)
    events.foreach { evt =>
      types add s"${evtCodec.eventName(evt)}:${evtCodec.eventVersion(evt)}"
      data add evtCodec.encodeEvent(evt)
    }
    types -> data
  }

  private val StreamExistsCheck =
    session.prepare(s"SELECT rev FROM $TableName WHERE stream = ? ORDER BY rev DESC LIMIT 1")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
  def currRevision(streamId: ID): Future[Option[Int]] = {
    execute(StreamExistsCheck, streamId)(rs => Option(rs.one).map(_.getInt(0)))
  }

  private val ReplayStream =
    session.prepare(s"SELECT $StreamSpecificColumns FROM $TableName WHERE stream = ? ORDER BY rev")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
  def replayStream(stream: ID)(callback: StreamCallback[Transaction]): Unit = {
    query(callback, ReplayStream, stream)
  }

  private val ReplayStreamFrom =
    session.prepare(s"SELECT $StreamSpecificColumns FROM $TableName WHERE stream = ? AND rev >= ? ORDER BY rev")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[Transaction]): Unit = {
    if (fromRevision == 0) {
      replayStream(stream)(callback)
    } else {
      query(callback, ReplayStreamFrom, stream, fromRevision)
    }
  }

  private val ReplayStreamTo =
    session.prepare(s"SELECT $StreamSpecificColumns FROM $TableName WHERE stream = ? AND rev <= ? ORDER BY rev")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
  def replayStreamTo(stream: ID, toRevision: Int)(callback: StreamCallback[Transaction]): Unit = {
    query(callback, ReplayStreamTo, stream, toRevision)
  }

  private val ReplayStreamRange =
    session.prepare(s"SELECT $StreamSpecificColumns FROM $TableName WHERE stream = ? AND rev >= ? AND rev <= ? ORDER BY rev")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
  def replayStreamRange(stream: ID, revisionRange: collection.immutable.Range)(callback: StreamCallback[Transaction]): Unit = {
    require(revisionRange.step == 1, s"Revision range must step by 1 only, not ${revisionRange.step}")
    val from = revisionRange.head
    val to = revisionRange.last
    if (from == to) {
      replayStreamRevision(stream, from)(callback)
    } else if (from == 0) {
      replayStreamTo(stream, to)(callback)
    } else {
      query(callback, ReplayStreamRange, stream, from, to)
    }
  }

  private val ReplayStreamRevision =
    session.prepare(s"SELECT $StreamSpecificColumns FROM $TableName WHERE stream = ? AND rev = ?")
      .setConsistencyLevel(ConsistencyLevel.SERIAL)
  private def replayStreamRevision(stream: ID, revision: Int)(callback: StreamCallback[Transaction]): Unit = {
    query(callback, ReplayStreamRevision, stream, revision)
  }

  private val RecordFirstRevision =
    session.prepare(s"INSERT INTO $TableName (stream, time, eventTypes, eventData, metadata, category, rev) VALUES(?,?,?,?,?,?,0) IF NOT EXISTS")
  private val RecordLaterRevision =
    session.prepare(s"INSERT INTO $TableName (stream, time, eventTypes, eventData, metadata, rev) VALUES(?,?,?,?,?,?) IF NOT EXISTS")

  protected def insert(clock: Long, categoryOrRevision: Either[Any, Int], stream: Any, evtTypes: JList[String], evtData: JList[String], metadata: JMap[String, String])(handler: ResultSet => Transaction): Future[Transaction] = {
    categoryOrRevision match {
      case Right(revision) =>
        execute(RecordLaterRevision, stream, clock, evtTypes, evtData, metadata, revision)(handler)
      case Left(category) =>
        execute(RecordFirstRevision, stream, clock, evtTypes, evtData, metadata, category)(handler)
    }
  }

  def createStream(streamId: ID, category: CAT, clock: Long, events: Seq[EVT], metadata: Map[String, String] = Map.empty): Future[Transaction] =
    record(clock, Left(category), streamId, events, metadata)
  def appendStream(streamId: ID, revision: Int, clock: Long, events: Seq[EVT], metadata: Map[String, String] = Map.empty): Future[Transaction] =
    record(clock, Right(revision), streamId, events, metadata)

  private def record(clock: Long, categoryOrRevision: Either[CAT, Int], stream: ID, events: Seq[EVT], metadata: Map[String, String]): Future[Transaction] = {
    val dbId = implicitly[TypeConverter[ID]].toCassandraType(stream)
    val dbCategoryOrRevision: Either[Any, Int] = categoryOrRevision.left.map(implicitly[TypeConverter[CAT]].toCassandraType)
    val (jTypes, jData) = toStringLists(events)
    val jMetadata = metadata.asJava
    insert(clock, dbCategoryOrRevision, dbId, jTypes, jData, jMetadata) { rs =>
      if (rs.wasApplied) {
        new Transaction(clock, category, stream, revision, metadata, events)
      } else {
        val conflicting = toTransaction(stream, rs.one, Columns(rs.getColumnDefinitions))
        throw new DuplicateRevisionException(conflicting)
      }
    }
  }

}

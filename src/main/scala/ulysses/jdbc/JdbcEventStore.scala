//package ulysses.jdbc
//
//import ulysses._
//import scala.reflect.{ ClassTag, classTag }
//import java.util.{ UUID, List => JList, ArrayList }
//import scuff.concurrent._
//import scala.concurrent._
//import scala.util.{ Try, Success, Failure }
//import collection.JavaConverters._
//import scuff.Multiton
//import collection.immutable.Seq
//import scuff.Codec
//import ulysses.util.EventCodec
//import java.sql.Connection
//
//private object JdbcEventStore {
//
//  trait Connected {
//    def apply[T](thunk: Connection => T): T
//  }
//  class DriverConnected()
//
//  private val CassandraTypes: Map[Class[_], String] = Map(
//    classOf[Array[Byte]] -> "BINARY",
//    classOf[UUID] -> "BINARY",
//    classOf[String] -> "VARCHAR",
//    classOf[Long] -> "BIGINT",
//    classOf[java.lang.Long] -> "BIGINT",
//    classOf[Int] -> "INT",
//    classOf[java.lang.Integer] -> "INT")
//
//  private def ensureTable[ID: TypeConverter, CH: TypeConverter](
//    session: Session, keyspace: String, table: String, replication: Map[String, Any]) {
//    val replicationStr = replication.map {
//      case (key, str: CharSequence) => s"'$key':'$str'"
//      case (key, any) => s"'$key':$any"
//    }.mkString("{", ",", "}")
//    def typeClass[T: TypeConverter]: Class[_] = implicitly[TypeConverter[ID]].cassandraType.runtimeClass
//    val idTypeStr = CassandraTypes.getOrElse(typeClass[ID], sys.error(s"Unsupported ID type: ${typeClass[ID].getName}"))
//    val catTypeStr = CassandraTypes.getOrElse(typeClass[CH], sys.error(s"Unsupported channel type: ${typeClass[CH].getName}"))
//    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStr")
//    session.execute(s"""CREATE TABLE IF NOT EXISTS $keyspace.$table (
//    		stream $idTypeStr,
//    		revision INT,
//    		time BIGINT,
//    		channel $catTypeStr,
//    		events LIST<TEXT>,
//    		metadata MAP<TEXT,TEXT>,
//    		PRIMARY KEY (stream, revision)
//    );""")
//    session.execute(s"CREATE INDEX IF NOT EXISTS ON $keyspace.$table(clock)")
//    session.execute(s"CREATE INDEX IF NOT EXISTS ON $keyspace.$table(channel)")
//  }
//
//}
//
///**
// * JDBC event store implementation.
// */
//class JdbcEventStore[ID: TypeConverter, EVT, CH: TypeConverter](
//  session: Session,
//  keyspace: String, table: String,
//  replication: Map[String, Any])(implicit exeCtx: ExecutionContext, evtCodec: EventCodec[EVT, String])
//    extends EventStore[ID, EVT, CH] {
//
//  CassandraEventStore.ensureTable[ID, CH](session, keyspace, table, replication)
//
//  private def getID(row: Row): ID = implicitly[TypeConverter[ID]].getValue(row, 0)
//  private def getChannel(row: Row): CH = implicitly[TypeConverter[CH]].getValue(row, 3)
//
//  private def toTransaction(row: Row): Transaction = {
//    val id = getID(row)
//    val clock = row.getLong("clock")
//    val channel = getChannel(row)
//    val revision = row.getInt("revision")
//    val metadata = {
//      val map = row.getMap("metadata", classOf[String], classOf[String])
//      if (map.isEmpty) {
//        Map.empty[String, String]
//      } else {
//        map.asScala.toMap
//      }
//    }
//    val events = fromStringList(row.getList("events", classOf[String]))
//    new Transaction(clock, channel, id, revision, metadata, events)
//  }
//
//  private def execute[T](stm: BoundStatement)(handler: ResultSet => T): Future[T] = {
//    val result = session.executeAsync(stm)
//    val promise = Promise[T]
//    val listener = new Runnable {
//      def run: Unit = promise complete Try(handler(result.get))
//    }
//    result.addListener(listener, exeCtx)
//    promise.future
//  }
//
//  private def query(callback: StreamCallback[Transaction], stm: PreparedStatement, parms: Any*) {
//    val refParms = parms.asInstanceOf[Seq[AnyRef]]
//    val bound = stm.bind(refParms: _*)
//    execute(bound) { rs =>
//      val iter = rs.iterator().asScala.map(toTransaction)
//      while (iter.hasNext) callback.onNext(iter.next)
//    }
//  }
//  private def execute[T](stm: PreparedStatement, parms: Any*)(handler: ResultSet => T): Future[T] = {
//    val refParms = parms.asInstanceOf[Seq[AnyRef]]
//    val bound = stm.bind(refParms: _*)
//    execute(bound)(handler)
//  }
//
//  protected def EvtFmtSepChar: Char = 29
//
//  private def fromStringList(list: JList[String]): Seq[EVT] =
//    list.iterator().asScala.foldLeft(Vector.empty[EVT]) {
//      case (events, str) =>
//        val sep1 = str.indexOf(EvtFmtSepChar)
//        val name = str.substring(0, sep1)
//        val sep2 = str.indexOf(EvtFmtSepChar, sep1 + 1)
//        val version = scuff.Numbers.parseUnsafeInt(str, sep1 + 1, sep2).toShort
//        events :+ evtCodec.decodeEvent(name, version, str.substring(sep2 + 1))
//    }
//  private def toStringList(events: Seq[EVT]): ArrayList[String] = {
//    events.foldLeft(new ArrayList[String](events.size)) {
//      case (list, evt) =>
//        val sb = new java.lang.StringBuilder()
//        sb append evtCodec.eventName(evt) append EvtFmtSepChar
//        sb append evtCodec.eventVersion(evt) append EvtFmtSepChar
//        sb append evtCodec.encodeEvent(evt)
//        list add sb.toString
//        list
//    }
//  }
//
//  private val StreamExistsCheck = session.prepare(s"SELECT revision FROM $keyspace.$table WHERE stream = ? ORDER BY revision DESC LIMIT 1")
//  def currRevision(streamId: ID): Future[Option[Int]] = {
//    execute(StreamExistsCheck, streamId)(rs => Option(rs.one).map(_.getInt(0)))
//  }
//  private val LastTimestamp = session.prepare(s"SELECT clock FROM $keyspace.$table ORDER BY clock DESC LIMIT 1")
//  def lastTimestamp: Future[Long] = {
//    execute(LastTimestamp)(rs => Option(rs.one).map(_.getLong(0)).getOrElse(Long.MinValue))
//  }
//
//  private val ReplayStream = session.prepare(s"SELECT * FROM $keyspace.$table WHERE stream = ? ORDER BY revision")
//  def replayStream(stream: ID)(callback: StreamCallback[Transaction]): Unit = {
//    query(callback, ReplayStream, stream)
//  }
//
//  private val ReplayStreamSince = session.prepare(s"SELECT * FROM $keyspace.$table WHERE stream = ? AND revision > ? ORDER BY revision")
//  def replayStreamAfter(stream: ID, afterRevision: Int)(callback: StreamCallback[Transaction]): Unit = {
//    query(callback, ReplayStreamSince, stream, afterRevision)
//  }
//
//  private val ReplayStreamTo = session.prepare(s"SELECT * FROM $keyspace.$table WHERE stream = ? AND revision <= ? ORDER BY revision")
//  def replayStreamTo(stream: ID, toRevision: Int)(callback: StreamCallback[Transaction]): Unit = {
//    query(callback, ReplayStreamTo, stream, toRevision)
//  }
//
//  private val ReplayStreamRange = session.prepare(s"SELECT * FROM $keyspace.$table WHERE stream = ? AND revision >= ? AND revision <= ? ORDER BY revision")
//  def replayStreamRange(stream: ID, revisionRange: collection.immutable.Range)(callback: StreamCallback[Transaction]): Unit = {
//    query(callback, ReplayStreamRange, stream, revisionRange.head, revisionRange.last)
//  }
//
//  private def newReplayStatement(channelCount: Int) = {
//    val cql = channelCount match {
//      case 0 =>
//        s"SELECT * FROM $keyspace.$table ORDER BY clock"
//      case 1 =>
//        s"SELECT * FROM $keyspace.$table WHERE channel = ? ORDER BY clock"
//      case n =>
//        val qs = Seq.fill(n)("?").mkString(",")
//        s"SELECT * FROM $keyspace.$table WHERE channel IN ($qs) ORDER BY clock"
//    }
//    session.prepare(cql)
//  }
//  private val Replay = new Multiton[Int, PreparedStatement](newReplayStatement)
//  def replay(channels: CH*)(callback: StreamCallback[Transaction]): Unit = {
//    val stm = Replay(channels.size)
//    query(callback, stm, channels: _*)
//  }
//
//  private def newReplayFromStatement(channelCount: Int) = {
//    val cql = channelCount match {
//      case 0 =>
//        s"SELECT * FROM $keyspace.$table WHERE clock >= ? ORDER BY clock"
//      case 1 =>
//        s"SELECT * FROM $keyspace.$table WHERE clock >= ? AND channel = ? ORDER BY clock"
//      case n =>
//        val qs = Seq.fill(n)("?").mkString(",")
//        s"SELECT * FROM $keyspace.$table WHERE clock >= ? AND channel IN ($qs) ORDER BY clock"
//    }
//    session.prepare(cql)
//  }
//  private val ReplayFrom = new Multiton[Int, PreparedStatement](newReplayFromStatement)
//  def replayFrom(fromTimestamp: Long, channels: CH*)(callback: StreamCallback[Transaction]): Unit = {
//    val stm = ReplayFrom(channels.size)
//    val parms = fromTimestamp +: channels
//    query(callback, stm, parms)
//  }
//
//  private val RecordTransaction =
//    session.prepare(s"INSERT INTO $keyspace.$table (clock, stream, revision, channel, events, metadata) VALUES(?,?,?,?,?,?) IF NOT EXISTS")
//  def record(clock: Long, channel: CH, stream: ID, revision: Int, events: Seq[EVT], metadata: Map[String, String]): Future[Transaction] = {
//    val jEvents = toStringList(events)
//    val jMetadata = metadata.asJava
//    execute(RecordTransaction, stream, revision, channel, jEvents, jMetadata) { rs =>
//      if (rs.wasApplied) {
//        new Transaction(clock, channel, stream, revision, metadata, events)
//      } else {
//        val conflicting = toTransaction(rs.one)
//        throw new DuplicateRevisionException(conflicting)
//      }
//    }
//  }
//
////  private val FetchLastRevision = session.prepare(s"SELECT revision FROM $keyspace.$table WHERE stream = ? ORDER BY revision DESC LIMIT 1")
//
//}
//

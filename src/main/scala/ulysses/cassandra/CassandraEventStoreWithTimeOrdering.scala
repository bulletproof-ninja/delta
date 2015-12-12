package ulysses.cassandra

import ulysses.TimeOrdering
import scala.concurrent.Future
import collection.immutable.{ Seq, NumericRange }
import ulysses.EventStore
import scala.util.Success
import com.datastax.driver.core.Session
import scala.concurrent.ExecutionContext
import ulysses.util.EventCodec
import scala.util.control.NonFatal
import java.util.{ List => JList, Map => JMap, ArrayList }
import com.datastax.driver.core.ResultSet
import ulysses.TimeOrdering
import scuff.concurrent._
import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Try
import com.datastax.driver.core.PreparedStatement
import scuff.Multiton
import scuff.Interval

private object CassandraEventStoreWithTimeOrdering {
  import CassandraEventStore._

  object Meta extends Enumeration {
    val MaxTime, MinTime = Value
  }

  private def ensureTable[ID: TypeConverter, CAT: TypeConverter](
    session: Session, keyspace: String, table: String) {
      def typeClass[T: TypeConverter]: Class[_] = implicitly[TypeConverter[ID]].cassandraType.runtimeClass
    val idTypeStr = CassandraTypes.get(typeClass[ID])
    val catTypeStr = CassandraTypes.get(typeClass[CAT])
    session.execute(s"""CREATE TABLE IF NOT EXISTS $keyspace.${table}_bytime (
        stream $idTypeStr,
        rev INT,
        timepart INT,
        time BIGINT,
        category $catTypeStr,
        eventTypes LIST<TEXT>,
        eventData LIST<TEXT>,
        metadata MAP<TEXT,TEXT>,
        PRIMARY KEY ((timepart), time, stream)
    ) WITH clustering ORDER BY (time ASC)
      AND caching = {'keys': 'NONE', 'rows_per_partition':'NONE'};""")
    session.execute(s"""CREATE INDEX IF NOT EXISTS ${table}_category ON $keyspace.${table}_bytime (category);""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS $keyspace.${table}_meta (
        key INT,
        long BIGINT,
        PRIMARY KEY ((key))
    );""")
  }

  @annotation.tailrec
  private def replaceHolders(stm: String, parms: Seq[Any]): String = parms match {
    case Nil => stm
    case head +: tail =>
      val replaceWith = head match {
        case str: CharSequence => s"'$str'"
        case _ => head.toString
      }
      replaceHolders(stm.replaceFirst("\\?", replaceWith), tail)
  }

  class DataConsistencyFailure(cause: Throwable, val cql: String, val parms: Seq[Any])
      extends RuntimeException(s"FAILED: ${replaceHolders(cql, parms)}", cause) {
  }

}
trait TableTimeDescriptor extends TableDescriptor {
  def timePartitionSize: Int
}

class CassandraEventStoreWithTimeOrdering[ID: TypeConverter, EVT, CAT: TypeConverter](
  session: Session,
  td: TableTimeDescriptor)(implicit exeCtx: ExecutionContext, evtCodec: EventCodec[EVT, String])
    extends CassandraEventStore[ID, EVT, CAT](session, td) with TimeOrdering[ID, EVT, CAT] {

  private[this] val idTC = implicitly[TypeConverter[ID]]
  private[this] val catTC = implicitly[TypeConverter[CAT]]

  import CassandraEventStoreWithTimeOrdering._

  ensureTable[ID, CAT](session, td.keyspace, td.table)

  /**
   * INSERT to time ordered table failed. This will cause data
   * inconsistency if not fixed.
   * This method can be overridden to do more than just report
   * the failure to execution context.
   */
  protected def timeInsertFailed(cause: Throwable, stm: PreparedStatement, parms: Seq[Any]) {
    exeCtx reportFailure new DataConsistencyFailure(cause, stm.getQueryString, parms)
  }

  private val RecordMaxTime =
    session.prepare(s"INSERT INTO ${TableName}_meta (key, long) VALUES(${Meta.MaxTime.id}, ?)")
  private val RecordMinTime =
    session.prepare(s"INSERT INTO ${TableName}_meta (key, long) VALUES(${Meta.MinTime.id}, ?) USING TIMESTAMP ?")

  private val RecordRevisionByTime =
    session.prepare(s"INSERT INTO ${TableName}_bytime (stream, rev, timepart, time, category, eventTypes, eventData, metadata) VALUES(?,?,?,?,?,?,?,?)")
  protected override def insert(clock: Long, category: Any, stream: Any, revision: Int, evtTypes: JList[String], evtData: JList[String], metadata: JMap[String, String])(handler: ResultSet => Transaction): Future[Transaction] = {
    val txn = super.insert(clock, category, stream, revision, evtTypes, evtData, metadata)(handler)
    txn.foreach { _ =>
      val timePartition = (clock / td.timePartitionSize).toInt
      val parms = Seq(stream, revision, timePartition, clock, category, evtTypes, evtData, metadata)
      execute(RecordRevisionByTime, parms: _*)(rs => ()).onFailure {
        case t: Throwable => timeInsertFailed(t, RecordRevisionByTime, parms)
      }
      execute(RecordMaxTime, clock)(rs => ()).onFailure {
        case NonFatal(e) => exeCtx reportFailure e
      }
      if (timePartition <= minTimePartition) {
        minTimePartition = timePartition
        execute(RecordMinTime, clock, -clock)(rs => ()).onFailure {
          case NonFatal(e) => exeCtx reportFailure e
        }
      }
    }
    txn
  }

  private val QueryMaxTime =
    session.prepare(s"SELECT long FROM ${TableName}_meta WHERE key = ${Meta.MaxTime.id}")
  def latestTimestamp(): Future[Option[Long]] =
    execute(QueryMaxTime) { rs =>
      Option(rs.one).map(_.getLong(0))
    }
  private val QueryMinTime =
    session.prepare(s"SELECT long FROM ${TableName}_meta WHERE key = ${Meta.MinTime.id}")
  private def earliestTimestamp(): Future[Option[Long]] =
    execute(QueryMinTime) { rs =>
      Option(rs.one).map(_.getLong(0))
    }
  @volatile private var minTimePartition: Int = {
    session.execute(QueryMinTime.getQueryString).one match {
      case null => Int.MaxValue
      case row => (row.getLong(0) / td.timePartitionSize).toInt
    }
  }

  /**
    * Play back transactions to a given time (inclusive),
    * optionally filtered by one or more categories.
    * Callback is guaranteed to be called in strict time order.
    * @param categories Optional categories
    * @param callback Callback function
    */
  def replayTo(toTimestamp: Long, categories: CAT*)(callback: StreamCallback[Transaction]): Unit =
    earliestTimestamp() onComplete {
      case Failure(e) => callback.onError(e)
      case Success(None) => callback.onCompleted()
      case Success(Some(earliest)) =>
        replayRange(earliest -> toTimestamp, categories: _*)(callback)
    }

  /**
    * Play back events for all instances from a given time forward,
    * optionally filtered by one or more categories.
    * Callback is guaranteed to be called in strict time order.
    * @param fromTimestamp Only play back transactions since the provided clock (inclusive).
    * @param categories Optional categories
    * @param callback Callback function
    */
  def replayFrom(fromTimestamp: Long, categories: CAT*)(callback: StreamCallback[Transaction]): Unit =
    latestTimestamp() onComplete {
      case Failure(e) => callback.onError(e)
      case Success(None) => callback.onCompleted()
      case Success(Some(latest)) =>
        replayRange(fromTimestamp -> latest, categories: _*)(callback)
    }

  private def addCategoryFilter(baseStm: String, categoryCount: Int): String = {
    categoryCount match {
      case 0 => baseStm
      case 1 => s"$baseStm AND category = ?"
      case n => s"$baseStm AND category in (${(1 to n).map(_ => '?').mkString(",")})"
    }
  }
  private val ReplayFullTimePartitionCQL = s"SELECT ${CassandraEventStore.StreamSpecificColumns}, stream FROM ${TableName}_bytime WHERE timepart = ?"
  private val ReplayFullTimePartition = new Multiton[Int, PreparedStatement](catCount =>
    session.prepare(addCategoryFilter(ReplayFullTimePartitionCQL, catCount))
  )
  private val ReplayFirstTimePartition = new Multiton[Int, PreparedStatement](catCount =>
    session.prepare(addCategoryFilter(s"${ReplayFullTimePartitionCQL} AND time >= ?", catCount))
  )
  private val ReplayLastTimePartition = new Multiton[Int, PreparedStatement](catCount =>
    session.prepare(addCategoryFilter(s"${ReplayFullTimePartitionCQL} AND time <= ?", catCount))
  )
  private val ReplaySingleTimePartition = new Multiton[Int, PreparedStatement](catCount =>
    session.prepare(addCategoryFilter(s"${ReplayFullTimePartitionCQL}  AND time >= ? AND time <= ?", catCount))
  )

  private def replayPartitions(first: Long, last: Long, partitions: Stream[Int], categories: List[catTC.CT], callback: StreamCallback[Transaction]): Unit = {
      def whenCompleted(remaining: Stream[Int])(res: Try[Try[Unit]]): Unit = res.flatten match {
        case Success(_) => replayPartitions(first, last, remaining, categories, callback)
        case Failure(e) => callback.onError(e)
      }
      def processResultSet(rs: ResultSet): Try[Unit] = Try {
        val iter = rs.iterator().asScala map { row =>
          val stream = idTC.getValue(row, 6)
          toTransaction(stream, row, CassandraEventStore.FixedColumns)
        }
        while (iter.hasNext) callback.onNext(iter.next)
      }
    partitions match {
      case Stream.Empty => callback.onCompleted()
      case partition #:: tailParts =>
        val isFirst = (first / td.timePartitionSize) == partition
        if (isFirst && tailParts.isEmpty) { // Single partition
          execute(ReplaySingleTimePartition(categories.size), partition, first, last, categories: _*)(processResultSet).onComplete(whenCompleted(tailParts))
        } else if (isFirst) { // First partition
          execute(ReplayFirstTimePartition(categories.size), partition, first, categories: _*)(processResultSet).onComplete(whenCompleted(tailParts))
        } else if (tailParts.nonEmpty) { // Middle partition
          execute(ReplayFullTimePartition(categories.size), partition, categories: _*)(processResultSet).onComplete(whenCompleted(tailParts))
        } else { // Last partition
          execute(ReplayLastTimePartition(categories.size), partition, last, categories: _*)(processResultSet).onComplete(whenCompleted(tailParts))
        }
    }
  }

  def replayRange(range: (Long, Long), categories: CAT*)(callback: StreamCallback[Transaction]): Unit = {
    val (first, last) = range
    require(first <= last, s"Must have positive time range: $first to $last")
    val dbCategories = categories.map(catTC.toCassandraType).toList
    val firstPartition = (first / td.timePartitionSize).toInt
    val lastPartition = (last / td.timePartitionSize).toInt
    val partitions = Stream.range(firstPartition, lastPartition + 1)
    replayPartitions(first, last, partitions, dbCategories, callback)
  }

}

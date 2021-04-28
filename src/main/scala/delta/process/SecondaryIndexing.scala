package delta.process

import scala.concurrent.Future
import scuff.Reduction
import scuff.concurrent.LockFreeConcurrentMap

/**
  * Optional trait for [[delta.process.StreamProcessStore]]
  * implementations that support secondary indexes.
  */
trait SecondaryIndexing {
  store: StreamProcessStore[_, _, _] =>

  /** Generalized query value type. */
  protected type QueryType

  /** General bulk read mechanism. */
  protected def bulkRead[R](
      select: (String, QueryType)*)(
      consumer: Reduction[(StreamId, Snapshot), R])
      : Future[R]

  /**
    * Read snapshots matching column value(s),
    * using `AND` semantics, so multiple column
    * queries should not use mutually exclusive
    * values.
    * @note This is intended to read one or at most a few snapshots. Use `bulkRead` for
    * larger datasets.
    * @param select The (first) selection criteria name of column or field and associated value to match
    * @param and Addtional `select`s, applied with `AND` semantics
    * @return `Map` of stream ids and snapshot
    */
  protected def readSnapshots(
      select: (String, QueryType), and: (String, QueryType)*)
      : Future[Map[StreamId, Snapshot]] =
    bulkRead((select +: and): _*) {
      new Reduction[(StreamId, Snapshot), Map[StreamId, Snapshot]] {
        // Use the LockFreeConcurrentMap, because we don't have to copy for result
        private[this] val map = new LockFreeConcurrentMap[Any, Snapshot]
        def next(t: (StreamId, Snapshot)): Unit =
          map.update(t._1, t._2)

        def result(): Map[StreamId, Snapshot] =
          map.snapshot

      }
    }

  /**
    * Lighter version of `readSnapshots` if only existence and/or tick
    * is needed.
    * @note This is intended to read one or at most a few snapshots.
    * @see [[readSnapshots]]
    * @param select The (first) selection criteria name of column or field and associated value to match
    * @param and Addtional `select`s, applied with `AND` semantics
    * @return `Map` of stream ids and tick (in case of duplicates, for chronology)
    */
  protected def readTicks(
      select: (String, QueryType), and: (String, QueryType)*)
      : Future[Map[StreamId, Long]]

}

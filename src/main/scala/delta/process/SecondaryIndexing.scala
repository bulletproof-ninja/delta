package delta.process

import scala.concurrent.Future

/**
  * Optional trait for [[delta.process.StreamProcessStore]]
  * implementations that support secondary indexes.
  */
trait SecondaryIndexing {
  store: StreamProcessStore[_, _, _] =>

  /** Generalized query value type. */
  protected type QueryType

  /**
    * Query for snapshots matching column value(s).
    * Uses `AND` semantics, so multiple column
    * queries should not use mutually exclusive
    * values.
    * @param nameValue The name of column or field and associated value to match
    * @param more Addtional `nameValue`s, applied with `AND` semantics
    * @return `Map` of stream ids and snapshot
    */
  protected def queryForSnapshot(
      nameValue: (String, QueryType), more: (String, QueryType)*)
      : Future[Map[StreamId, Snapshot]]

  /**
    * Lighter version of `queryForSnapshot` if only existence and/or tick
    * is needed.
    * Uses `AND` semantics, so multiple column
    * queries should not use mutually exclusive
    * values.
    * @param nameValue The name of column or field and associated value to match
    * @param more Addtional `nameValue`s, applied with `AND` semantics
    * @return `Map` of stream ids and tick (in case of duplicates, for chronology)
    */
  protected def queryForTick(
      nameValue: (String, QueryType), more: (String, QueryType)*)
      : Future[Map[StreamId, Long]]

}

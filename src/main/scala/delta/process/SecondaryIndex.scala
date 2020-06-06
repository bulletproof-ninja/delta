package delta.process

import scala.concurrent.Future

trait SecondaryIndex {
  store: StreamProcessStore[_, _, _] =>

  /** Generalized value type. */
  protected type QueryValue

  /**
    * Query for snapshots matching column values.
    * Uses `AND` semantics, so multiple column
    * queries should not use mutually exclusive
    * values.
    * @param nameValue The name of column or field and associated value to match
    * @param more Addtional `nameValue`s, applied with `AND` semantics
    * @return `Map` of stream ids and snapshot
    */
  protected def querySnapshot(
      nameValue: (String, QueryValue), more: (String, QueryValue)*)
      : Future[Map[StreamId, Snapshot]]

  /**
    * Lighter version of `querySnapshot` if only existence needed.
    * Uses `AND` semantics, so multiple column
    * queries should not use mutually exclusive
    * values.
    * @param nameValue The name of column or field and associated value to match
    * @param more Addtional `nameValue`s, applied with `AND` semantics
    * @return `Map` of stream ids and tick (in case of duplicates, for chronology)
    */
  protected def queryTick(
      nameValue: (String, QueryValue), more: (String, QueryValue)*)
      : Future[Map[StreamId, Long]]

}

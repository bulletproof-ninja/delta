package delta.process

import scala.concurrent.Future

/**
  * Optional trait for [[delta.process.StreamProcessStore]]
  * implementations that aggregation.
  */
trait AggregationSupport {
  store: StreamProcessStore[_, _, _] =>

  /** Meta type. */
  protected type MetaType[R]

  /**
    * Find duplicates, if any.
    * @tparam D Non-unique type
    * @param refName Reference name
    * @return `Map` of duplicates, if any
    */
  protected def findDuplicates[D](
      refName: String)(
      implicit metaType: MetaType[D]): Future[Map[D, Map[StreamId, Tick]]]

}

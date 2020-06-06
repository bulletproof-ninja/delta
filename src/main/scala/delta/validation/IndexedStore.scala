package delta.validation

import delta.process.StreamProcessStore
import scala.concurrent.Future

trait IndexedStore {
  store: StreamProcessStore[_, _, _] =>

  protected type Ref[V]

  /**
    * Find duplicates in index, if any
    * @return `Map` of duplicates
    */
  protected def findDuplicates[V](
      refName: String)(
      implicit refType: Ref[V]): Future[Map[V, Map[StreamId, Tick]]]

}

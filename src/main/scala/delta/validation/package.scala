package delta

import scala.concurrent.Future

/**
  * Support for global, cross stream, state validation
  * and compensating (rollback) actions.
  * @see https://docs.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction
  */
package object validation {

  /** Compensation function. */
  type Compensate[C] = C => write.Metadata

  private[this] val future_EmptyMap = Future successful Map.empty[Any, Any]

  private[validation] def Future_EmptyMap[A, B]  =
    future_EmptyMap.asInstanceOf[Future[Map[A, B]]]

  /**
    * Get oldest entry, determined by lowest `Tick` value.
    * @param candidates Map of candidates.
    * @return Oldest key
    */
  def pickOldest[K](candidates: collection.Map[K, delta.Tick]): Option[K] =
    if (candidates.isEmpty) None
    else Some(pickOldestNonEmpty(candidates))

  /**
    * Get oldest entry, determined by lowest `Tick` value.
    * @param candidates Map of candidates.
    * @return Oldest key
    */
  private[validation] def pickOldestNonEmpty[K](candidates: collection.Map[K, delta.Tick]): K =
    candidates.toList.sortBy(_._2).head._1

}

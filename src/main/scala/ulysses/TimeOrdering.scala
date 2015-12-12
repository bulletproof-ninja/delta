package ulysses

import scuff.concurrent.StreamCallback
import scala.concurrent.Future
import scala.collection.immutable.NumericRange
import scuff.Interval

trait TimeOrdering[ID, EVT, CAT] {
  es: EventSource[ID, EVT, CAT] =>

  /**
    * Highest clock recorded.
    */
  def maxClock(): Future[Option[Long]]
  /**
    * Lowest clock recorded.
    */
  def minClock(): Future[Option[Long]]

  /**
    * Play back transactions to a given time (inclusive),
    * optionally filtered by one or more categories.
    * Callback is guaranteed to be called in strict time order.
    * @param toTimestamp Only play back transactions before provided clock (inclusive).
    * @param categories Optional categories
    * @param callback Callback function
    */
  def replayTo(toTimestamp: Long, categories: CAT*)(callback: StreamCallback[Transaction]): Unit

  /**
    * Play back events for all instances from a given time forward,
    * optionally filtered by one or more categories.
    * Callback is guaranteed to be called in strict time order.
    * @param fromTimestamp Only play back transactions since the provided clock (inclusive).
    * @param categories Optional categories
    * @param callback Callback function
    */
  def replayFrom(fromTimestamp: Long, categories: CAT*)(callback: StreamCallback[Transaction]): Unit

  /**
    * Play back events for all instances from a given time range,
    * filtered by one or more categories.
    * Callback is guaranteed to be called in strict time order.
    * @param range Only play back transactions between the provided range (both inclusive).
    * @param categories Optional categories
    * @param callback Callback function
    */
  def replayRange(range: (Long, Long), categories: CAT*)(callback: StreamCallback[Transaction]): Unit

}

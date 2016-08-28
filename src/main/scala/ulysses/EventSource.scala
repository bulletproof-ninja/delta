package ulysses

import scala.concurrent.Future
import scuff.concurrent.StreamCallback

/**
  * Event source.
  */
trait EventSource[ID, EVT, CH] {

  type TXN = Transaction[ID, EVT, CH]
  def currRevision(stream: ID): Future[Option[Int]]
  def lastTick(): Future[Option[Long]]

  def replayStream(stream: ID)(callback: StreamCallback[TXN]): Unit
  def replayStreamRange(stream: ID, revisionRange: Range)(callback: StreamCallback[TXN]): Unit
  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[TXN]): Unit
  def replayStreamTo(stream: ID, toRevision: Int)(callback: StreamCallback[TXN]): Unit =
    replayStreamRange(stream, 0 to toRevision)(callback)

  def replay(filter: StreamFilter[ID, EVT, CH] = StreamFilter.Everything())(callback: StreamCallback[TXN]): Unit
  def replaySince(sinceTick: Long, filter: StreamFilter[ID, EVT, CH] = StreamFilter.Everything())(callback: StreamCallback[TXN]): Unit

}

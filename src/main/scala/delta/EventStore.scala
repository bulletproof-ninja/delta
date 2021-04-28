package delta

import scala.concurrent.Future
import scala.util.control.NoStackTrace
import scala.util.Success
import java.util.concurrent.CopyOnWriteArrayList
import scuff.Subscription
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/**
 * Event store. Extension of [[delta.EventSource]] that
 * allows committing event transactions.
 * @tparam ID Stream id type
 * @tparam EVT Event type
 */
trait EventStore[ID, EVT]
extends EventSource[ID, EVT] {

  def ticker: Ticker

  final class DuplicateRevisionException(val conflict: Transaction)
    extends RuntimeException(s"Revision ${conflict.revision} already exists for: ${conflict.stream}")
    with NoStackTrace {
    override def toString = super[RuntimeException].toString()
  }

  /**
   * Commit transaction.
   * @param stream Stream identifier.
   * @param revision Stream revision.
   * @param events The events, at least one.
   * @param metadata Optional metadata
   * @return Transaction, or if failed a possible
   * [[DuplicateRevisionException]] if the revision already exists.
   */
  final def commit(channel: Channel, stream: ID, revision: Revision,
      events: List[EVT], metadata: Map[String, String] = Map.empty)
      : Future[Transaction] =
    this.commit(channel, stream, revision, ticker.nextTick(), events, metadata)

  /**
   * Commit transaction.
   * @param stream Stream identifier.
   * @param revision Stream revision.
   * @param tick The clock tick
   * @param events The events, at least one.
   * @param metadata Metadata
   * @return Transaction, or if failed a possible
   * [[DuplicateRevisionException]] if the revision already exists.
   */
  final def commit(channel: Channel, stream: ID, revision: Revision,
      tick: Tick, events: List[EVT], metadata: Map[String, String])
      : Future[Transaction] =
    commit(tick, channel, stream, revision, metadata, events)
      .andThen {
        case Success(tx) =>
          publish(tx)
      }(publishCtx)

  /**
    * Context on which subscribers are invoked.
    * @note Supports [[scuff.concurrent.PartitionedExecutionContext]] on stream identifier
    */
  protected def publishCtx: ExecutionContext

  protected def publish(tx: Transaction): Unit = {
    subscribers.forEach {
      _.runnableOrNull(tx) match {
        case null =>
          // Not subscribed
        case runnable =>
          try publishCtx execute runnable
          catch {
            case NonFatal(th) =>
              publishCtx reportFailure th
          }
      }
    }

  }

  private final class Subscriber(
    selector: this.StreamsSelector,
    callback: Transaction => Any) {

    def runnableOrNull(tx: Transaction): Runnable =
      if (selector include tx)
        new Runnable {
          def run(): Unit =
            try callback(tx) catch {
              case NonFatal(cause) =>
                subscribers remove Subscriber.this
                publishCtx reportFailure cause
            }
          override def hashCode(): Int = tx.stream.##
        }
      else null

  }

  private val subscribers = new CopyOnWriteArrayList[Subscriber]

  def subscribeLocal[U](selector: StreamsSelector)(callback: Transaction => U): Subscription = {
    val subscriber = new Subscriber(selector, callback)
    subscribers add subscriber
    new Subscription {
      def cancel(): Unit = subscribers remove subscriber
    }
  }

  protected def commit(
    tick: Tick,
    channel: Channel,
    stream: ID,
    revision: Revision,
    metadata: Map[String, String],
    events: List[EVT])
    : Future[Transaction]

}

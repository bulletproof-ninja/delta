package delta.util

import scala.concurrent.ExecutionContext
import scuff.concurrent.MultiMap
import scala.concurrent.Future
import scuff.Subscription

/**
  * @tparam ID The id type
  * @tparam S The state type
  * @tparam FMT The Snapshot notification type
  */
trait NotificationSupport[ID, S >: Null, FMT] {
  consumer: DefaultEventSourceConsumer[ID, _, S] =>

  private class Subscriber(val id: ID, thunk: PartialFunction[(ID, Snapshot, FMT), Unit], notificationCtx: ExecutionContext) {
    // Best effort to avoid race condition, while not incurring unnecessary cost
    @volatile private[this] var blind: Boolean = true
    def onUpdate(snapshot: Snapshot, formatted: FMT): Unit = {
      val input = (id, snapshot, formatted)
      if (thunk isDefinedAt input) {
        blind = false
        notificationCtx execute new Runnable {
          def run = thunk(input)
          override def hashCode = id.##
        }
      }
    }
    def onLatest(snapshot: Snapshot, formatted: FMT): Unit =
      if (blind) {
        val input = (id, snapshot, formatted)
        notificationCtx execute new Runnable {
          def run = if (blind) thunk(input)
          override def hashCode = id.##
        }
      }
  }

  private[this] val subscribers = new MultiMap[ID, Subscriber]

  protected def contentUpdatesOnly: Boolean = false
  protected def format(id: ID, snapshot: Snapshot, contentUpdated: Boolean): FMT
  protected def store: StreamProcessStore[ID, S]

  protected def onSnapshotUpdate(id: ID, snapshot: Snapshot, contentUpdated: Boolean): Unit = {
    if (contentUpdated || !contentUpdatesOnly) {
      val subs = subscribers(id)
      if (subs.nonEmpty) {
        val formatted = format(id, snapshot, contentUpdated)
        subs.foreach(_.onUpdate(snapshot, formatted))
      }
    }
  }

  /**
    * Subscribe to latest state.
    * NOTE: If id exists, subscriber will always get the
    * latest state automatically.
    */
  protected def subscribe(notificationCtx: ExecutionContext)(ids: Iterable[ID])(thunk: PartialFunction[(ID, Snapshot, FMT), Unit]): Future[Subscription] = {
    require(ids.nonEmpty, "Must have at least one id to subscribe")

    val subs = ids.map { id =>
      val sub = new Subscriber(id, thunk, notificationCtx)
      subscribers(id) += sub
      id -> sub
    }.toMap
    store.readBatch(subs.keys).map { result =>
      result.foreach {
        case (id, latest) => subs(id).onLatest(latest, format(id, latest, true))
      }
      new Subscription {
        def cancel = subs.foreach {
          case (id, sub) => subscribers(id) -= sub
        }
      }
    }(notificationCtx)

  }
}

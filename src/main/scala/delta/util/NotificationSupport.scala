package delta.util

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure

import scuff.Subscription
import scuff.concurrent.MultiMap

/**
  * @tparam ID The id type
  * @tparam S The state type
  * @tparam PF The Snapshot publish format
  */
trait NotificationSupport[ID, S >: Null, PF] {
  consumer: DefaultEventSourceConsumer[ID, _, S] =>

  private type Subscriber = NotificationSubscriber[ID, S, PF, Callback]
  private[this] val subscribers = new TrieMap[FormatKey, MultiMap[ID, Subscriber]]

  /**
    * Publish format key type.
    */
  protected type FormatKey
  protected def contentUpdatesOnly: Boolean = false
  protected def format(key: FormatKey, id: ID, snapshot: Snapshot, contentUpdated: Boolean): PF
  protected def store: StreamProcessStore[ID, S]

  protected def onSnapshotUpdate(id: ID, snapshot: Snapshot, contentUpdated: Boolean): Unit = {
    if (contentUpdated || !contentUpdatesOnly) {
      subscribers.foreach {
        case (fmtKey, subscribers) =>
          val subs = subscribers(id)
          if (subs.nonEmpty) {
            val formatted = format(fmtKey, id, snapshot, contentUpdated)
            subs.foreach(_.onUpdate((id, snapshot, formatted)))
          }
      }
    }
  }

  type Callback = (ID, Snapshot, PF)

  private def unsubscribe(fmtKey: FormatKey, subs: Map[ID, Subscriber]): Unit = {
    val subscribers = this.subscribers(fmtKey)
    subs.foreach {
      case (id, sub) => subscribers(id) -= sub
    }
  }

  /**
    * Subscribe to latest state.
    * NOTE: If id currently exists,
    * subscriber will always get the
    * latest state automatically.
    */
  protected def subscribe(notificationCtx: ExecutionContext)(ids: Iterable[ID], fmtKey: FormatKey)(callback: PartialFunction[Callback, Unit]): Future[Subscription] = {
    require(ids.nonEmpty, "Must have at least one id to subscribe")
    val subscribers = this.subscribers.get(fmtKey) match {
      case Some(subscribers) =>
        subscribers
      case None =>
        val multimap = new MultiMap[ID, Subscriber]
        this.subscribers.putIfAbsent(fmtKey, multimap) getOrElse multimap
    }
    val subById = ids.map { id =>
      val sub = new Subscriber(id, notificationCtx)(callback)
      subscribers(id) += sub
      id -> sub
    }.toMap

      implicit def ec = notificationCtx

    store.readBatch(ids)
      .andThen {
        case Failure(_) => unsubscribe(fmtKey, subById)
      } map { result =>
        subById.foreach {
          case (id, sub) =>
            val initialState = result.get(id).map { snapshot =>
              (id, snapshot, format(fmtKey, id, snapshot, true))
            }
            sub.onInitial(initialState)
        }
        new Subscription {
          def cancel = unsubscribe(fmtKey, subById)
        }
      }

  }
}

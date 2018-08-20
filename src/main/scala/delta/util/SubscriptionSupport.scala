package delta.util

import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.util.Failure

import scuff.Subscription
import scuff.concurrent.MultiMap
import java.util.concurrent.ArrayBlockingQueue
import delta.ddd.Revision
import delta.Snapshot

/**
  * @tparam ID The id type
  * @tparam S The state type
  * @tparam PF The Snapshot publish format
  */
trait SubscriptionSupport[ID, S >: Null, PF] {
  consumer: EventSourceConsumer[ID, _] =>

  private type Subscriber = NotificationSubscriber[ID, S, PF, Callback]
  private[this] val subscribers = new TrieMap[FormatKey, MultiMap[ID, Subscriber]]

  /**
    * Publish format key type.
    */
  protected type FormatKey
  protected def contentUpdatesOnly: Boolean = false
  protected def format(key: FormatKey, id: ID, snapshot: Snapshot[S], contentUpdated: Boolean): PF
  protected def store: StreamProcessStore[ID, S]

  protected def onSnapshotUpdate(id: ID, snapshot: Snapshot[S], contentUpdated: Boolean): Unit = {
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

  type Callback = (ID, Snapshot[S], PF)

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
              (id, snapshot, format(fmtKey, id, snapshot, contentUpdated = true /* full rendering */))
            }
            sub.onInitial(initialState)
        }
        new Subscription {
          def cancel = unsubscribe(fmtKey, subById)
        }
      }

  }

  protected def lookupRevision(notificationCtx: ExecutionContext)(id: ID, expectedRevision: Revision, fmtKey: FormatKey): Future[delta.Snapshot[PF]] = {
    val promise = Promise[delta.Snapshot[PF]]
    val subscription = new ArrayBlockingQueue[Future[Subscription]](1)
      def cancelSubscription(): Unit = {
        notificationCtx execute new Runnable {
          def run = blocking {
            subscription.take().foreach(_.cancel)(notificationCtx)
          }
        }
      }
    subscription offer subscribe(notificationCtx)(id :: Nil, fmtKey) {
      case (_, snapshot @ Snapshot(_, revision, _), formattedContent) =>
        if (expectedRevision matches snapshot.revision) {

          val promiseFulfilled = promise trySuccess snapshot.copy(content = formattedContent)
          if (promiseFulfilled) cancelSubscription()

        } else expectedRevision match {
          case Revision.Exactly(expectedRevision) if expectedRevision < revision && !promise.isCompleted =>

            val promiseFulfilled = promise tryFailure new IllegalStateException(s"Expected revision $expectedRevision for $id is stale and cannot be retrieved. Current revision is $revision. Try using Minimum revision?")
            if (promiseFulfilled) cancelSubscription()

          case _ => // Ignore (keep subscription active)
        }
    }
    promise.future
  }
}

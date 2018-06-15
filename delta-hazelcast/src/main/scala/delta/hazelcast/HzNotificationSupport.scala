package delta.hazelcast

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.HashSet
import scala.concurrent.{ ExecutionContext, Future }

import com.hazelcast.core.EntryEvent
import com.hazelcast.map.listener.{ EntryAddedListener, EntryMergedListener, EntryUpdatedListener, MapListener }

import scuff.Subscription
import delta.Snapshot
import com.hazelcast.map.EntryProcessor
import java.util.Map.Entry
import scala.util.Success
import scala.util.Failure
import delta.util.NotificationSubscriber

/**
  * @tparam ID The id type
  * @tparam WS The working state type
  * @tparam RS The resting state type
  * @tparam PF The Snapshot notification type
  */
trait HzNotificationSupport[ID, WS >: Null, RS >: Null, PF >: Null] {
  consumer: HzEventSourceConsumer[ID, _, WS, RS] =>

  private object EntryListener extends MapListener
    with EntryAddedListener[ID, Any]
    with EntryMergedListener[ID, Any]
    with EntryUpdatedListener[ID, Any] {

    private def onUpdate(id: ID): Unit = {
        implicit def ec = executionContext
      subscriptions.foreach {
        case (key, subscriptions) =>
          subscriptions.lookup(id) match {
            case null => // No subscribers => race condition, should be rare
            case subs =>
              val formatter = publishFormatter(key)
              val callback = CallbackPromise.option[Snapshot[PF]]
              imap.submitToKey(id, formatter, callback)
              callback.future.onComplete {
                case Success(snapshot) => snapshot.foreach { snapshot =>
                  subs.subscribers.foreach(sub => sub.onUpdate((sub.id, snapshot)))
                }
                case Failure(th) => reportFailure(th)
              }
          }
      }
    }

    def entryAdded(evt: EntryEvent[ID, Any]): Unit = onUpdate(evt.getKey)
    def entryMerged(evt: EntryEvent[ID, Any]): Unit = onUpdate(evt.getKey)
    def entryUpdated(evt: EntryEvent[ID, Any]): Unit = onUpdate(evt.getKey)
  }

  type Callback = (ID, Snapshot[PF])
  private type Subscriber = NotificationSubscriber[ID, RS, PF, Callback]
  private class Subscribers private (imapListenerReg: String, val subscribers: HashSet[Subscriber]) {
    def this(first: Subscriber) = this(imap.addEntryListener(EntryListener, first.id, false), HashSet(first))
    def update(updatedSubscribers: HashSet[Subscriber]) = {
      assert {
        val id = this.subscribers.head.id
        updatedSubscribers.forall(_.id == id)
      }
      new Subscribers(imapListenerReg, updatedSubscribers)
    }
    def unlisten(): Unit = {
      imap.removeEntryListener(imapListenerReg)
    }
  }
  private[this] val subscriptions = new TrieMap[FormatKey, TrieMap[ID, Subscribers]]

//  private def addListener(id: ID): String = {
//    val regId = imap.addEntryListener(EntryListener, id, false)
//    new Subscription {
//      def cancel = {
//        try imap.removeEntryListener(regId) catch {
//          case NonFatal(th) => reportFailure(th)
//        }
//      }
//    }
//  }

  @annotation.tailrec
  private def unsubscribe(fmtKey: FormatKey, sub: Subscriber): Unit = {
    val subscriptions = this.subscriptions(fmtKey)
    val existingSubscriptions = subscriptions(sub.id)
    import existingSubscriptions.subscribers
    val removed = subscribers - sub
    if (removed.isEmpty) {
      if (subscriptions.remove(sub.id, existingSubscriptions)) {
        existingSubscriptions.unlisten()
      } else {
        unsubscribe(fmtKey, sub)
      }
    } else {
      val updatedSubscriptions = existingSubscriptions.update(removed)
      if (!subscriptions.replace(sub.id, existingSubscriptions, updatedSubscriptions)) {
        unsubscribe(fmtKey, sub)
      }
    }
  }

  @annotation.tailrec
  private def subscribe(fmtKey: FormatKey, sub: Subscriber): Subscriber = {
    val subscriptions = this.subscriptions.get(fmtKey) match {
      case Some(subscriptions) => subscriptions
      case None =>
        val subs = new TrieMap[ID, Subscribers]
        this.subscriptions.putIfAbsent(fmtKey, subs) getOrElse subs
    }
      def addSubscriber(existingSubscribers: Subscribers): Boolean = {
        val updatedSubscribers = existingSubscribers.update(existingSubscribers.subscribers + sub)
        subscriptions.replace(sub.id, existingSubscribers, updatedSubscribers)
      }
    subscriptions.lookup(sub.id) match {
      case null =>
        val newSubscribers = new Subscribers(sub)
        subscriptions.putIfAbsent(sub.id, newSubscribers) match {
          case None => sub
          case Some(existingSubscribers) =>
            newSubscribers.unlisten()
            if (addSubscriber(existingSubscribers)) sub
            else subscribe(fmtKey, sub)
        }
      case existingSubscribers =>
        if (addSubscriber(existingSubscribers)) sub
        else subscribe(fmtKey, sub)
    }
  }

  protected type FormatKey
  protected type PublishFormatter = HzNotificationSupport.PublishFormatter[ID, RS, PF]
  protected def publishFormatter(key: FormatKey): PublishFormatter

  protected def lookup(id: ID, key: FormatKey): Future[Option[Snapshot[PF]]] = lookup(id, publishFormatter(key))
  protected def lookup(id: ID, formatter: PublishFormatter): Future[Option[Snapshot[PF]]] = {
    val callback = CallbackPromise.option[Snapshot[PF]]
    imap.submitToKey(id, formatter, callback)
    callback.future
  }

  /**
    * Subscribe to latest state.
    * NOTE: If id exists, subscriber will always get the
    * latest state automatically.
    */
  protected def subscribe(notificationCtx: ExecutionContext)(ids: Iterable[ID], fmtKey: FormatKey)(callback: PartialFunction[Callback, Unit]): Future[Subscription] = {
      implicit def ec = executionContext

    require(ids.nonEmpty, "Must have at least one id to subscribe")

    val subs = ids.map { id =>
      val sub = new Subscriber(id, notificationCtx)(callback)
      subscribe(fmtKey, sub)
    }
    val formatter = publishFormatter(fmtKey)
    val futures = subs.map { sub =>
      lookup(sub.id, formatter).map { snapshot =>
        sub.onInitial(snapshot.map(sub.id -> _))
      }
    }
    (Future sequence futures)
      .andThen {
        case Failure(_) => subs.foreach(unsubscribe(fmtKey, _))
      } map { _ =>
        new Subscription {
          def cancel = subs.foreach(unsubscribe(fmtKey, _))
        }
      }
  }
}

object HzNotificationSupport {

  abstract class PublishFormatter[ID, S, PF >: Null]
    extends EntryProcessor[ID, EntryState[S, _]] {

    final def getBackupProcessor = null
    final def process(entry: Entry[ID, EntryState[S, _]]): Object = {
      entry.getValue match {
        case null => null
        case EntryState(snapshot, contentUpdated, _) =>
          if (snapshot == null) null
          else snapshot.copy(content = format(entry.getKey, snapshot, contentUpdated))
      }
    }
    final def format(id: ID, entryState: EntryState[S, _]): Option[Snapshot[PF]] =
      Option(entryState.snapshot).map { snapshot =>
        snapshot.copy(content = format(id, snapshot, entryState.contentUpdated))
      }

    def format(id: ID, snapshot: Snapshot[S], contentUpdated: Boolean): PF

  }

}

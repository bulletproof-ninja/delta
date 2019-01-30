package delta

import java.util.concurrent.{ ScheduledExecutorService, ScheduledFuture }
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.annotation.varargs
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

import scuff.{ Codec, FakeType, Memoizer, Subscription }
import scuff.concurrent.{ ScuffLock, ScuffScheduledExecutor }

object MessageHub {
  type Namespace = Namespace.Type
  val Namespace: FakeType[String] = new FakeType[String] {
    type Type = String
    def apply(str: String) = str
  }
}

/**
 * Message hub.
 *
 * @tparam MSG Message type
 */
trait MessageHub[MSG] {

  type Namespace = MessageHub.Namespace

  /** Message publishing format. */
  type PublishFormat

  /** Message codec. */
  protected def messageCodec: Codec[MSG, PublishFormat]

  /** The execution context to publish on. */
  protected def publishCtx: ExecutionContext

  /**
   * Publish message implementation. This will happen on
   * the `publishCtx` execution context.
   * @param ns Publishing namespace
   * @param msg Formatted message
   */
  protected def publishImpl(ns: Namespace, msg: PublishFormat): Unit

  /**
   * Publish future message.
   * @param ns Publishing namespace
   * @param msg Future message
   */
  final def publish(ns: Namespace, msg: Future[MSG]): Unit = {
    msg.foreach { msg =>
      try publishImpl(ns, messageCodec encode msg) catch {
        case NonFatal(e) => publishCtx reportFailure e
      }
    }(publishCtx)
  }

  /**
   * Publish message.
   * @param ns Publishing namespace
   * @param msg message
   */
  final def publish(ns: Namespace, msg: MSG): Unit =
    publishCtx execute new Runnable {
      def run = publishImpl(ns, messageCodec encode msg)
    }

  /**
   *  Subscription key, identifies scope of subscription.
   *  NOTE: Must be an immutable value type.
   */
  protected type SubscriptionKey
  /**
   *  Define subscription key(s) from requested namespace(s).
   */
  protected def subscriptionKeys(nss: Set[Namespace]): Set[SubscriptionKey]
  /**
   *  Subscribe with given subscription key.
   *  @param key Subscription key
   *  @param callback Namespace / wire-format callback
   */
  protected def subscribeToKey(key: SubscriptionKey)(callback: (Namespace, PublishFormat) => Unit): Subscription

  protected final class Subscriber(nss: Set[Namespace], callback: PartialFunction[MSG, Any]) {
    @inline def matches(ns: Namespace) = nss contains ns
    @inline def notifyIfMatch(msg: MSG): Unit = if (callback isDefinedAt msg) callback(msg)
  }

  final def subscribe[U](nss: java.lang.Iterable[Namespace], callback: java.util.function.Consumer[_ >: MSG]): Subscription = {
    import scala.collection.JavaConverters._
    subscribe(nss.asScala) {
      case msg => callback accept msg
    }
  }

  final def subscribe[U](nss: Iterable[Namespace])(callback: PartialFunction[MSG, U]): Subscription = {
    require(nss.nonEmpty, "Must subscribe at least one namespace")
    subscribe(nss.head, nss.tail.toSeq: _*)(callback)
  }

  /**
   * Subscribe to messages.
   * @param ns Subscribe to namespace
   * @param moreNS Optional additional namespace(s) to include in subscription
   * @param callback Message callback
   */
  @varargs
  def subscribe[U](ns: Namespace, moreNS: Namespace*)(callback: PartialFunction[MSG, U]): Subscription = {
    val nss = (ns +: moreNS).toSet
    val keys = subscriptionKeys(nss)
    require(keys.nonEmpty, "Method 'subscriptionKeys' must return at least one subscription; was empty")
    val subscriber = new Subscriber(nss, callback)
    val internalCallback: (Namespace, PublishFormat) => Unit = {
      case (ch, wf) if (subscriber matches ch) =>
        val msg = messageCodec decode wf
        subscriber notifyIfMatch msg
      case _ => // Ignore message, not in ns
    }
    val subscriptions = keys.toList.map { key =>
      subscribeToKey(key)(internalCallback)
    }
    new Subscription {
      def cancel(): Unit = subscriptions.foreach(s => Try(s.cancel))
    }
  }

}

/**
 * Apply this trait to [[MessageHub]] implementations,
 * if individual subscriptions lead to inefficient use
 * of resources.
 */
trait SubscriptionPooling[MSG] {
  hub: MessageHub[MSG] =>

  /** Optional delay in propagating pooled subscription cancellation. */
  protected def cancellationDelay: Option[(ScheduledExecutorService, FiniteDuration)]

  private final class PooledSubscription(key: SubscriptionKey) {
    import scuff.concurrent._
    private[this] var subscription: Option[Subscription] = None
    private[this] var scheduledCancellation: Option[ScheduledFuture[_]] = None
    private[this] val subscribers: collection.mutable.Buffer[Subscriber] = collection.mutable.Buffer()
    private[this] val (sharedLock, exclusiveLock) = {
      val rwLock = new ReentrantReadWriteLock
      (rwLock.readLock, rwLock.writeLock)
    }
    private[this] def cancelSubscription(): Unit = exclusiveLock {
      subscription.foreach { subscription =>
        try subscription.cancel() catch {
          case NonFatal(th) => publishCtx.reportFailure(th)
        }
        this.subscription = None
      }
    }

    private[this] def notifySubscribers(ns: Namespace, pf: PublishFormat): Unit = sharedLock {
      val subscribers = this.subscribers.iterator.filter(_ matches ns)
      if (subscribers.nonEmpty) {
        val msg = messageCodec decode pf
        subscribers.foreach(_ notifyIfMatch msg)
      }
    }

    def subscribe(sub: Subscriber): Subscription = {
      exclusiveLock {
        scheduledCancellation.foreach { scheduledCancellation =>
          scheduledCancellation.cancel( /* mayInterruptIfRunning = */ false)
          this.scheduledCancellation = None
        }
        if (subscription.isEmpty) {
          assert(subscribers.isEmpty)
          val sub = subscribeToKey(key)(notifySubscribers)
          this.subscription = Some(sub)
        }
        subscribers += sub
      }
      new Subscription {
        def cancel() = exclusiveLock {
          subscribers -= sub
          if (subscribers.isEmpty) {
            scheduledCancellation = cancellationDelay map {
              case (scheduler, delay) =>
                scheduler.schedule(delay)(cancelSubscription)
            }
            if (scheduledCancellation.isEmpty) {
              cancelSubscription()
            }
          }
        }
      }
    }
  }

  private[this] val pooledSubscriptions = new Memoizer[SubscriptionKey, PooledSubscription](new PooledSubscription(_))

  @varargs
  override def subscribe[U](ns: Namespace, moreNS: Namespace*)(callback: PartialFunction[MSG, U]): Subscription = {
    val nss = (ns +: moreNS).toSet
    val keys = subscriptionKeys(nss)
    val subscriber = new Subscriber(nss, callback)
    val subscriptions = keys.toList.map { key =>
      val pooled = pooledSubscriptions(key)
      pooled.subscribe(subscriber)
    }
    new Subscription {
      def cancel() = subscriptions.foreach(s => Try(s.cancel))
    }
  }

}

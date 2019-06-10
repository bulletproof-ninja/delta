package delta

import java.util.concurrent.{ ScheduledExecutorService, ScheduledFuture }
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.annotation.varargs
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.{ Try, Success, Failure }
import scala.util.control.NonFatal

import scuff.{ FakeType, Memoizer, Subscription, Numbers }
import scuff.concurrent._
import java.util.concurrent.BlockingQueue
import scuff.concurrent.Threads
import scala.util.control.NoStackTrace
import scala.reflect.{ ClassTag, classTag }

object MessageHub {

  type Topic = Topic.Type
  val Topic: FakeType[String] = new FakeType[String] {
    type Type = String
    def apply(str: String) = str
  }

  val DefaultBackoff = new Iterable[FiniteDuration] {
    def iterator =
      Numbers.fibonacci.iterator
        .dropWhile(_ < 1) // At least 2 seconds
        .takeWhile(_ < 90) // At most 90 seconds
        .map(_.seconds)
  }

  private[delta] val ThreadGroup = Threads.newThreadGroup(classOf[MessageHub].getName, daemon = false)
  final class PublishFailure(topic: Topic, msg: Any, cause: Throwable)
    extends RuntimeException(s"Failed to publish '$topic': $msg", cause)

}

/**
 * Message hub.
 *
 * @tparam Msg Message type
 */
trait MessageHub {

  /** Native message type. */
  type MsgType

  type Topic = MessageHub.Topic
  def Topic(name: String): Topic = MessageHub.Topic(name)

  /** The execution context to publish on. */
  protected def publishCtx: ExecutionContext

  /**
   * Publish message implementation. This will happen on
   * the `publishCtx` execution context.
   * @param topic Publishing topic
   * @param msg Hub-native message
   */
  protected def publishImpl(topic: Topic, msg: MsgType): Unit

  /**
   * Publish future message.
   * @param topic Publishing topic
   * @param msg Future message
   */
  final def publish[M](
      topic: Topic, msg: Future[M])(
      implicit
      encoder: M => MsgType): Unit = {
    msg.foreach { msg =>
      try publish(topic, msg) catch {
        case NonFatal(cause) =>
          publishCtx reportFailure new MessageHub.PublishFailure(topic, msg, cause)
      }
    }(publishCtx)
  }

  /**
   * Publish message.
   * @param topic Publishing topic
   * @param msg message
   */
  def publish[M](topic: Topic, msg: M)(
      implicit
      encoder: M => MsgType): Unit =
    publishCtx execute new Runnable {
      def run = try publishImpl(topic, encoder(msg)) catch {
        case NonFatal(cause) =>
          publishCtx reportFailure new MessageHub.PublishFailure(topic, msg, cause)
      }
    }

  /**
   *  Subscription key, identifies scope of subscription.
   *  NOTE: Must be an immutable value type.
   */
  protected type SubscriptionKey

  /**
   *  Define subscription key(s) from requested topic(s).
   */
  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey]
  /**
   *  Subscribe with given subscription key.
   *  @param key Subscription key
   *  @param callback Topic + MsgType callback
   */
  protected def subscribeToKey(key: SubscriptionKey)(callback: (Topic, MsgType) => Unit): Subscription

  protected final class Subscriber[M: ClassTag](topics: Set[Topic], callback: PartialFunction[M, Unit]) {
    @inline def matches[M2: ClassTag](topic: Topic) = (topics contains topic) && (classTag[M] == classTag[M2])
    @inline def notifyIfMatch(msg: M): Unit = if (callback isDefinedAt msg) callback(msg)
  }

  final def subscribe[M](msgType: Class[M], decoder: MsgType => M, topics: java.lang.Iterable[Topic], callback: java.util.function.Consumer[_ >: M]): Subscription = {
    import scala.collection.JavaConverters._
    implicit val tag = ClassTag[M](msgType)
    subscribe[M](topics.asScala) {
      case msg: M => callback accept msg
    }(tag, decoder)
  }

  /**
   * Subscribe to messages.
   * @param topics One or more topics to subscribe to
   * @param callback Message callback
   * @param decoder Implicit decoder
   */
  def subscribe[M: ClassTag](
      topics: Iterable[Topic])(
      callback: PartialFunction[M, Unit])(
      implicit
      decoder: MsgType => M): Subscription = {

    require(topics.nonEmpty, "Must subscribe at least one topic")

    val topicSet = topics.toSet
    val keys = subscriptionKeys(topicSet)
    require(keys.nonEmpty, "Method 'subscriptionKeys' must return at least one key; was empty")
    val subscriber = new Subscriber(topicSet, callback)
    val internalCallback: (Topic, MsgType) => Unit = {
      case (topic, ht) if (subscriber matches topic) =>
        val msg = decoder(ht)
        subscriber notifyIfMatch msg
      case _ => // Ignore message, not in topic
    }
    val subscriptions = keys.toList.map { key =>
      subscribeToKey(key)(internalCallback)
    }
    new Subscription {
      def cancel(): Unit = subscriptions.foreach(s => Try(s.cancel))
    }

  }

  /**
   * Subscribe to messages.
   * @param topic Subscribe to topic
   * @param moreTopics Additional topics, if any
   * @param callback Message callback
   */
  @varargs
  final def subscribe[M: ClassTag](
      topic: Topic, moreTopics: Topic*)(
      callback: PartialFunction[M, Unit])(
      implicit
      decoder: MsgType => M): Subscription =
    subscribe[M](topic +: moreTopics)(callback)

}

/**
 * Apply this trait to [[MessageHub]] implementations
 * as a buffer against failed publish failures, with
 * automatic retry.
 * NOTE: After publish failures, messages may get
 * re-ordered thus eventually delivered out of order.
 * This can be mitigated by using a
 * `java.util.concurrent.PriorityBlockingQueue`
 * implementation, if necessary.
 */
trait BufferedRetryPublish {
  hub: MessageHub =>

  /** The publish queue. */
  protected def publishQueue: BlockingQueue[(Topic, MsgType)]
  /** The threshold before circuit breaker is tripped. */
  protected def circuitBreakerThreshold: Int
  /** The retry back-off schedule circuit breaker. */
  protected def publishFailureBackoff: Iterable[FiniteDuration]

  override def publish[M](topic: Topic, msg: M)(
      implicit
      encoder: M => MsgType): Unit = {
    if (!publishThread.isAlive) publishThread.start()
    enqueue(topic, encoder(msg))
  }

  private def enqueue(topic: Topic, msg: MsgType): Unit = {
    try publishQueue add topic -> msg catch {
      case NonFatal(cause) => // If enqueuing fails, for any reason, report it, but don't propagate failure
        publishCtx reportFailure new MessageHub.PublishFailure(topic, msg, cause)
    }
  }

  private class PublishDelay(delay: FiniteDuration)
    extends RuntimeException(s"Failed to publish. Will retry again in $delay")
    with NoStackTrace

  private val publisherThreadGroup = Threads.newThreadGroup(s"${getClass.getName}:publisher", daemon = false, MessageHub.ThreadGroup, publishCtx.reportFailure)

  private[this] val publishThread = new Thread(publisherThreadGroup, s"${publisherThreadGroup.getName}:${getClass.getName}") {
    override def run: Unit = {
      try {
        publishMessages()
      } catch {
        case _: InterruptedException => Thread.currentThread.interrupt()
        case NonFatal(cause) => publishCtx reportFailure cause
      }
    }
    private def publishMessages() = {
      val ft = new FailureTracker(circuitBreakerThreshold, publishCtx.reportFailure, publishFailureBackoff)
      while (!Thread.currentThread.isInterrupted) {
        val timeout = ft.timeout()
        if (timeout.length > 0) {
          publishCtx reportFailure new PublishDelay(timeout)
          timeout.unit.sleep(timeout.length)
        }
        val (topic, msg) = publishQueue.take() // blocking
        publishCtx execute new Runnable {
          def run = try {
            publishImpl(topic, msg)
            ft.reset() // Publish success
          } catch {
            case NonFatal(cause) =>
              ft reportFailure cause
              enqueue(topic, msg)
          }
        }
      }
    }
  }

}

/**
 * Apply this trait to [[MessageHub]] implementations,
 * if individual subscriptions lead to inefficient use
 * of resources.
 * NOTE: It is assumed that individual subscriber decoding
 * of a given `MsgType` are identical, such that decoding only
 * happens, at most, once per message, not per subscriber.
 */
trait SubscriptionPooling {
  hub: MessageHub =>

  /** Optional delay in propagating pooled subscription cancellation. */
  protected def cancellationDelay: Option[(ScheduledExecutorService, FiniteDuration)]

  private final class PooledSubscription[M: ClassTag](key: SubscriptionKey) {
    import scuff.concurrent._
    private[this] var subscription: Option[Subscription] = None
    private[this] var scheduledCancellation: Option[ScheduledFuture[_]] = None
    private[this] val subscribers: collection.mutable.Buffer[Subscriber[M]] = collection.mutable.Buffer()
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

    private[this] def notifySubscribers(decoder: MsgType => M)(topic: Topic, hubMsg: MsgType): Unit = {
      val subscribers = sharedLock {
        this.subscribers.iterator.filter(_ matches topic).toArray
      }
      if (subscribers.nonEmpty) {
        val msg = decoder(hubMsg)
        subscribers.foreach(_ notifyIfMatch msg)
      }
    }

    def subscribe(sub: Subscriber[M], decoder: MsgType => M): Subscription = {
      exclusiveLock {
        scheduledCancellation.foreach { scheduledCancellation =>
          scheduledCancellation.cancel( /* mayInterruptIfRunning = */ false)
          this.scheduledCancellation = None
        }
        if (subscription.isEmpty) {
          assert(subscribers.isEmpty)
          val callback: (Topic, MsgType) => Unit = notifySubscribers(decoder) _
          val sub = subscribeToKey(key)(callback)
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

  private[this] val pooledSubscriptions = new Memoizer[SubscriptionKey, PooledSubscription[_]](new PooledSubscription(_))

  override def subscribe[M: ClassTag](
      topics: Iterable[Topic])(
      callback: PartialFunction[M, Unit])(
      implicit
      decoder: MsgType => M): Subscription = {

    require(topics.nonEmpty, "Must subscribe at least one topic")

    val topicSet = topics.toSet
    val keys = subscriptionKeys(topicSet)
    val subscriber = new Subscriber[M](topicSet, callback)
    val subscriptions = keys.toList.map { key =>
      pooledSubscriptions(key) match {
        case pooled: PooledSubscription[M] =>
          pooled.subscribe(subscriber, decoder)
      }
    }
    new Subscription {
      def cancel() = subscriptions.foreach(s => Try(s.cancel))
    }
  }

}

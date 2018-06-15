package delta.util

import scala.concurrent._
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch

private[delta] class NotificationSubscriber[ID, S, PF, CB](
    val id: ID,
    notificationCtx: ExecutionContext,
    maxInitialWait: FiniteDuration = 5.seconds)(
    callback: PartialFunction[CB, Unit]) {

  private[this] val initialLatch = new CountDownLatch(1)
  private val currLifetimeMs: () => Long = {
    val created = System.currentTimeMillis
    () => System.currentTimeMillis - created
  }

  def onUpdate(cbState: CB): Unit = {
    if (callback isDefinedAt cbState) {
      notificationCtx execute new Runnable {
        override def hashCode = id.##
        def run = {
          if (initialLatch.getCount != 0) {
            val remainingWaitTimeMs = maxInitialWait.toMillis - currLifetimeMs()
            if (remainingWaitTimeMs > 0L) blocking {
              if (!initialLatch.await(remainingWaitTimeMs, MILLISECONDS)) {
                initialLatch.countDown() // Give up, release latch
              }
            }
          }
          callback(cbState)
        }
      }
    }
  }
  def onInitialFailed(): Unit = initialLatch.countDown()
  def onInitial(initialState: Option[CB]): Unit = initialState match {
    case None => initialLatch.countDown()
    case Some(cbState) =>
      if (initialLatch.getCount != 0) {
        notificationCtx execute new Runnable {
          def run = try callback(cbState) finally initialLatch.countDown()
          override def hashCode = id.##
        }
      }
  }

}

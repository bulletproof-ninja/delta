package delta.java

import java.util.concurrent.Executor
import java.util.function.{ BiConsumer, Consumer }

import scala.concurrent.{ ExecutionContext, Future }

import delta.read.{ ReadModel, SubscriptionSupport }
import scuff.Subscription

trait SubscriptionAdapter[ID, View, U] {
  rm: ReadModel[ID, View] with SubscriptionSupport[ID, View, U] =>

  /**
   * Subscribe to snapshot updates with an initial snapshot.
   * NOTE: The callback will receiver *either* snapshot or update, never both.
   * In other words, the first callback will be a snapshot and all subsequent
   * callbacks will be updates.
   */
  def subscribe(
      id: ID, callbackEC: ExecutionContext,
      callback: BiConsumer[Snapshot, Update]): Future[Subscription] = {
    this.subscribe(id, callbackEC) {
      case Right(update) => callback.accept(null, update)
      case Left(snapshot) => callback.accept(snapshot, null)
    }
  }

  /**
   * Subscribe to snapshot updates with an initial snapshot.
   * NOTE: The callback will receiver *either* snapshot or update, never both.
   * In other words, the first callback will be a snapshot and all subsequent
   * callbacks will be updates.
   */
  def subscribe(
      id: ID, callbackExe: Executor, reportFailure: Consumer[Throwable],
      callback: BiConsumer[Snapshot, Update]): Future[Subscription] = {

    val callbackEC = ExecutionContext.fromExecutor(callbackExe, reportFailure.accept)
    this.subscribe(id, callbackEC) {
      case Right(update) => callback.accept(null, update)
      case Left(snapshot) => callback.accept(snapshot, null)
    }

  }

}

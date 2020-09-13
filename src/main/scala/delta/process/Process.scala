package delta.process

import scuff.Subscription
import scala.concurrent.Future
import scuff.concurrent.Threads

sealed trait Process {
  def name: String
}

trait ReplayProcess[F]
extends Process
with ReplayStatus {
  /** @return Future, which is available when replay is finished */
  def finished: Future[F]
  override def toString() = s"ReplayProcess($name)"
}
object ReplayProcess {
  def apply[T](
      status: ReplayStatus, future: Future[T])
      : ReplayProcess[T] =
    new ReplayProcess[T] {
      def finished = future
      def name = status.name
      def activeTransactions: Int = status.activeTransactions
      def totalTransactions: Long = status.totalTransactions
    }

  def apply[F, T](
      proc: ReplayProcess[F])(
      map: F => T)
      : ReplayProcess[T] =
    new ReplayProcess[T] {
      val finished = proc.finished.map(map)(Threads.PiggyBack)
      def name = proc.name
      def activeTransactions: Int = proc.activeTransactions
      def totalTransactions: Long = proc.totalTransactions
    }
}

trait LiveProcess
extends Process
with Subscription {
  override def toString() = s"LiveProcess($name)"
}

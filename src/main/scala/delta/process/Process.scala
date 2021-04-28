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
  def map[T](fromTo: F => T): ReplayProcess[T] = {
    val self = this
    new ReplayProcess[T] {
      def finished: Future[T] = self.finished.map(fromTo)(Threads.PiggyBack)
      def name = self.name
      def totalTransactions = self.totalTransactions
      def activeTransactions = self.activeTransactions
      def numErrors = self.numErrors
    }
  }
}
object ReplayProcess {
  def failed[T](process: String, cause: Throwable): ReplayProcess[T] =
    new ReplayProcess[T] {
      def name: String = process
      def totalTransactions: Long = 0
      def activeTransactions: Int = 0
      def numErrors: Int = 0
      val finished: Future[T] = Future failed cause
    }
  def apply[T](
      status: ReplayStatus, future: Future[T])
      : ReplayProcess[T] =
    new ReplayProcess[T] {
      def finished = future
      def name = status.name
      def activeTransactions: Int = status.activeTransactions
      def totalTransactions: Long = status.totalTransactions
      def numErrors: Int = status.numErrors
    }

}

trait LiveProcess
extends Process
with Subscription {
  override def toString() = s"LiveProcess($name)"
}

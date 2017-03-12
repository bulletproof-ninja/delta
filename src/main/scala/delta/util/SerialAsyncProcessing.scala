package delta.util

import scuff.concurrent.Threads
import scuff.concurrent.PartitionedExecutionContext
import delta.Transaction
import scala.concurrent.Future

/**
  * Ensure single-threaded processing of
  * transactions.
  */
final class SerialAsyncProcessing[TXN <: Transaction[_, _, _]](
  processor: TXN => Unit,
  failureReporter: Throwable => Unit,
  numThreads: Int = 1)
    extends (TXN => Unit) {

  require(numThreads > 0, s"Must have at least one thread, was given $numThreads")

  private[this] val exeCtx = {
    val threadFactory = {
      val thisName = getClass.getName
      val threadName = s"${classOf[SerialAsyncProcessing[_]].getName}($thisName)"
      Threads.factory(threadName)
    }
    PartitionedExecutionContext(numThreads, threadFactory, _.hashCode, failureReporter)
  }

  def apply(txn: TXN): Unit = exeCtx execute new Runnable {
    def run = processor(txn)
    override def hashCode = txn.stream.hashCode()
  }

  def shutdown(): Future[Unit] = exeCtx.shutdown()
}

package delta.testing

import scala.concurrent._
import scala.util.Random
import delta.EventStore

class RandomDelayExecutionContext(exeCtx: ExecutionContext, maxDelayMs: Int) extends ExecutionContext {

  def execute(runnable: Runnable) = exeCtx execute new Runnable {
    def run() = {
      if (Random.nextBoolean) {
        val delay = Random.nextInt(maxDelayMs)
        Thread sleep delay
      }
      runnable.run()
    }
  }

  def reportFailure(th: Throwable) = th match {
    case _: EventStore[_, _]#DuplicateRevisionException => // suppress
    case _ => exeCtx reportFailure th
  }

}

object RandomDelayExecutionContext
  extends RandomDelayExecutionContext(ExecutionContext.global, 50)

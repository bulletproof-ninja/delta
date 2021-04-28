package delta.testing

import scala.concurrent._
import scala.util.Random

class RandomDelayExecutionContext(maxDelayMs: Int = 50, exeCtx: ExecutionContext = ExecutionContext.global)
extends ExecutionContext {

  def execute(runnable: Runnable) = exeCtx execute new Runnable {
    def run() = {
      if (Random.nextBoolean()) {
        val delay = Random.nextInt(maxDelayMs)
        Thread sleep delay
      }
      runnable.run()
    }
  }
  def reportFailure(th: Throwable) = th match {
    case _ => th.printStackTrace(System.err)
  }
}

object RandomDelayExecutionContext
extends RandomDelayExecutionContext(50, ExecutionContext.global)

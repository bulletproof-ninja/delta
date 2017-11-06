package delta.testing

import scuff._
import scala.concurrent._
import scala.util.Random

class RandomDelayExecutionContext(exeCtx: ExecutionContext) extends ExecutionContext {

  def execute(runnable: Runnable) = exeCtx execute new Runnable {
    def run {
      if (Random.nextBoolean) {
        val delay = Random.nextInRange(1 to 50)
        Thread sleep delay
      }
      runnable.run()
    }
  }

  def reportFailure(th: Throwable) = exeCtx reportFailure th

}

object RandomDelayExecutionContext
  extends RandomDelayExecutionContext(ExecutionContext.global)

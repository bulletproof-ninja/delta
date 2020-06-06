package delta.validation

import scala.concurrent._

trait Compensation[SID, S] {

  type Tick = delta.Tick

  def ifNeeded(stream: SID, tick: Tick, state: S)(
      implicit
      ec: ExecutionContext): Future[Outcome]

}

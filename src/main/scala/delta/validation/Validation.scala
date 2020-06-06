package delta.validation

import scala.concurrent._

/**
  * A specific validation and compensation encapsulation.
  * @tparam ID Identifier
  * @tparam S State
  * @tparam Ctx Compensation context
  */
trait Validation[ID, S, Ctx] {

  type Tick = delta.Tick
  
  /** Validate and return compensation function, if needed. */
  def validate(
      id: ID, tick: Tick, state: S)(
      implicit
      validationContext: ExecutionContext)
      : Future[Option[Compensate[Ctx]]]

}

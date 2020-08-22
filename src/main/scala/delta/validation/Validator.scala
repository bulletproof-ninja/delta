package delta.validation

import scala.concurrent._

/**
  * Validation and lazy compensation encapsulation.
  *
  * @see [[delta.validation.GlobalSetValidator]] for a partial implementation
  *
  * @tparam ID Identifier
  * @tparam S State representation
  * @tparam Ctx Compensation context
  */
trait Validator[ID, S, Ctx] {

  type Tick = delta.Tick
  type Snapshot = delta.Snapshot[S]

  /** Validate and return compensation function, if needed. */
  def validate(
      id: ID, snapshot: Snapshot)(
      implicit
      validationContext: ExecutionContext)
      : Future[Map[ID, Compensate[Ctx]]]

}

package delta.validation

import scala.concurrent._
import scala.util.Try

/**
  * Compensation abstraction. Before publishing a
  * new transaction, this type is asked to determine,
  * and execute, any needed compensating actions,
  * based on the provided snapshot state.
  *
  * @see [[delta.validation.EntityCompensation]] For an Entity specific implementation
  *
  * @tparam SID Stream identifier
  * @tparam S State type
  */
trait Compensation[SID, S] {

  type Revision = delta.Revision
  type Tick = delta.Tick
  type Snapshot = delta.Snapshot[S]

  /** Given some state, issue compensating action, if needed. */
  def ifNeeded(stream: SID, state: Snapshot)(
      implicit
      ec: ExecutionContext): Future[Map[SID, Try[Revision]]]

}

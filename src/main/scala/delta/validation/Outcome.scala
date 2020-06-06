package delta.validation

import delta._

/**
  * Validation outcome.
  */
sealed abstract class Outcome
case object NotNeeded extends Outcome
case class Compensated(revision: Revision) extends Outcome

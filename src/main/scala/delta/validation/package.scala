package delta

import scala.concurrent.Future

package object validation {

  /** Compensation function. */
  type Compensate[C] = C => Unit

  private[validation] val Future_NotNeeded = Future successful NotNeeded

}

package ulysses.ddd

import scala.collection.immutable.Seq

trait AggrRoot[AR, ID, EVT, S] {
  def apply(id: ID, state: S, concurrentEvents: Seq[EVT]): AR
  def newEvents(ar: AR): Seq[EVT]
  def currState(ar: AR): S
  def checkInvariants(ar: AR)
}

package ulysses.ddd

import scala.collection.immutable.{ Seq => iSeq }

/**
  * Entity (aggregate root) type-class.
  * @tparam E Entity type
  * @tparam ID Entity id type
  * @tparam EVT The event super type
  * @tparam S The state type
  */
trait Entity[E, ID, EVT, S <: AnyRef] {
  def newMutator(state: Option[S]): StateMutator[EVT, S]
  def init(id: ID, state: S, mergeEvents: iSeq[EVT]): E
  def getMutator(entity: E): StateMutator[EVT, S]
  def checkInvariants(state: S): Unit
}

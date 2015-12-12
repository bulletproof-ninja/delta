package ulysses.ddd.cqrs

import scuff.Faucet

trait DataStore {

  type R
  type RW

  def readOnly[T](func: R => T): T
  def readWrite[T](func: RW => T): T

}

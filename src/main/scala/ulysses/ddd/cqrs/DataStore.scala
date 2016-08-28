package ulysses.ddd.cqrs

trait DataStore {

  type R
  type RW

  def readOnly[T](func: R => T): T
  def readWrite[T](func: RW => T): T

}

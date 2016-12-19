package college.readmodels

trait KVStore[K, V] {
  def update(key: K)(thunk: V => V): V
  def insert(key: K, value: => V)
  def upsert(key: K, value: => V)(thunk: V => V): V
  def get(key: K): Option[V]
}

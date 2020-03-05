package delta.hazelcast

import delta.{ Transaction, Snapshot }
import scala.collection.immutable.TreeMap

final case class EntryState[S, +EVT](
  snapshot: Snapshot[S],
  contentUpdated: Boolean = false,
  unapplied: TreeMap[Int, Transaction[_, EVT]] = EntryState.EmptyUnapplied)

private[hazelcast] object EntryState {
  def EmptyUnapplied[EVT] = TreeMap.empty[Int, Transaction[_, EVT]]
}

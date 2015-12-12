package ulysses.ddd

import scuff.ddd._
import collection.immutable.{Vector, Seq}

/**
 * This class extends [[StateMutator]]
 * and retains all applied events.
 */
//class EventHandler[EVT, S](sm: StateMutator[EVT, S]) extends StateMutator[EVT, S] {
//  private[this] var _events = Vector.empty[EVT]
//  def apply(evt: EVT) {
//    sm.apply(evt)
//    _events :+= evt
//  }
//  def events: Seq[EVT] = _events
//  def state = sm.state
//}

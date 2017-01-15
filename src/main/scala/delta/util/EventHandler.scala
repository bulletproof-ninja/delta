package delta.util

import scuff.DoubleDispatch

/**
 * Convenience event handler interface.
 *
 * Example:
 * {{{
 *
 *    trait UserEventHandler extends EventHandler[UserEvent] {
 *      def on(evt: UserCreated): RT
 *    }
 *
 *    sealed trait UserEvent extends DoubleDispatch[UserEventHandler]
 *
 *     case class UserCreated()
 *       extends UserEvent { def dispatch(h: UserEventHandler): h.RT = h.on(this) }
 *
 * }}}
 *
 * NOTE: Because this interface uses `apply` to consume the top-level
 * event type, the specific implementation methods should use some other
 * name, conventionally `on`.
 */
trait EventHandler[EVT <: DoubleDispatch[EventHandler[EVT]]] {
  type RT
  def apply(evt: EVT): RT = evt.dispatch(this)
}

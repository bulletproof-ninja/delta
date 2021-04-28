package blogging.write.author.events

import scala.{ SerialVersionUID => version }

import blogging.BloggingEvent
import scuff.EmailAddress

sealed abstract class AuthorEvent
extends BloggingEvent {
  type Callback = AuthorEvents
}

trait AuthorEvents {
  type Return

  def on(evt: AuthorRegistered): Return
  def on(evt: EmailAddressAdded): Return
  def on(evt: EmailAddressRemoved): Return
  def on(evt: AuthorNameChanged): Return
}

@version(1)
case class AuthorRegistered(
  name: String,
  emailAddress: EmailAddress)
extends AuthorEvent { def dispatch(callback: AuthorEvents) = callback on this }

@version(1)
case class EmailAddressAdded(emailAddress: EmailAddress)
extends AuthorEvent { def dispatch(callback: AuthorEvents) = callback on this }

@version(1)
case class EmailAddressRemoved(emailAddress: EmailAddress)
extends AuthorEvent { def dispatch(callback: AuthorEvents) = callback on this }

@version(1)
case class AuthorNameChanged(newName: String)
extends AuthorEvent { def dispatch(callback: AuthorEvents) = callback on this }

package blogging.write.author.state

import blogging.write.author.events._
import scuff.EmailAddress

private[author] case class Author(
  name: String,
  emailAddresses: Set[EmailAddress])

object Projector
extends delta.Projector[Author, AuthorEvent] {
  def init(evt: AuthorEvent): Author = evt dispatch new EventProjector
  def next(state: Author, evt: AuthorEvent): Author = evt dispatch new EventProjector(state)
}

private class EventProjector(author: Author = null)
extends AuthorEvents {

  type Return = Author

  def on(evt: AuthorRegistered): Author = {
    require(author == null)
    new Author(evt.name, Set(evt.emailAddress))
  }

  def on(evt: EmailAddressAdded): Author =
    author.copy(emailAddresses = author.emailAddresses + evt.emailAddress)

  def on(evt: EmailAddressRemoved): Author =
    author.copy(emailAddresses = author.emailAddresses - evt.emailAddress)

  def on(evt: AuthorNameChanged) =
    author.copy(name = evt.newName)

}

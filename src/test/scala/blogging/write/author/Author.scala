package blogging.write.author

import events._

import blogging.AuthorID

import delta.write.Entity

object Author
extends Entity("author", state.Projector) {
  type Type = Author
  type Id = AuthorID
  protected def init(id: Id, state: StateRef, concurrentUpdates: List[Transaction]) = new Author(state)
  protected def StateRef(author: Author) = author.stateRef
  protected def validate(s: state.Author) = ()

  def apply(cmd: RegisterAuthor): Author = {
    val author = new Author
    author.stateRef apply AuthorRegistered(cmd.name, cmd.emailAddress)
    author
  }
}

final class Author(
  private val stateRef: Author.StateRef = Author.newStateRef()) {
  private[this] def author = stateRef.get

  def emailAddresses = author.emailAddresses

  def apply(cmd: AddEmailAddress): Unit = {
    if (!(author.emailAddresses contains cmd.emailAddress)) {
      stateRef apply EmailAddressAdded(cmd.emailAddress)
    }
  }

  def apply(cmd: RemoveEmailAddress): Unit = {
    if (author.emailAddresses contains cmd.emailAddress) {
      if ((author.emailAddresses - cmd.emailAddress).isEmpty) {
        throw new IllegalArgumentException("Must have at least one email address.")
      } else {
        stateRef apply EmailAddressRemoved(cmd.emailAddress)
      }
    }
  }

  def apply(cmd: UpdateEmailAddress): Unit = {
    require(author.emailAddresses contains cmd.from, s"Unknown email address: ${cmd.from}")
    // TODO: Determine the impact of remove-then-add vs add-then-remove in a single transaction
    stateRef apply EmailAddressRemoved(cmd.from)
    if (!(author.emailAddresses contains cmd.to)) {
      stateRef apply EmailAddressAdded(cmd.to)
    }
  }
}

package blogging.write.author

import scuff.EmailAddress

case class RegisterAuthor(
  name: String,
  emailAddress: EmailAddress)

case class AddEmailAddress(
  emailAddress: EmailAddress)

case class RemoveEmailAddress(
  emailAddress: EmailAddress)

case class UpdateEmailAddress(
  from: EmailAddress, to: EmailAddress)

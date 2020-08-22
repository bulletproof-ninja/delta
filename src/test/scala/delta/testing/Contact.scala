package delta.testing

import scuff.EmailAddress
import scala.util.Random

final case class Contact(
  email: EmailAddress = EmailAddress(randomName, "email.COM"),
  num: Int = Random.nextInt())

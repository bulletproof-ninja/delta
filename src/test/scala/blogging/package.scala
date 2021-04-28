
import scuff.FakeType
import java.{util => ju}
import java.util.UUID

package blogging {

  sealed abstract class OpaqueUUID extends FakeType[ju.UUID] {
    type Type >: Null
    def uuid(id: Type): ju.UUID
    def newID(): Type
  }

}

package object blogging {
  type AuthorID = AuthorID.Type
  val AuthorID: OpaqueUUID = new OpaqueUUID {
    type Type = ju.UUID
    def apply(id: ju.UUID) = id
    def uuid(id: Type) = id
    def newID() = UUID.randomUUID()
  }

  type BlogPostID = BlogPostID.Type
  val BlogPostID: OpaqueUUID = new OpaqueUUID {
    type Type = ju.UUID
    def apply(id: ju.UUID) = id
    def uuid(id: Type) = id
    def newID() = UUID.randomUUID()
  }
  implicit def toUUID(id: OpaqueUUID#Type) = id.asInstanceOf[ju.UUID]

  implicit class AuthorIDOps(private val id: AuthorID) extends AnyVal {
    def uuid: ju.UUID = AuthorID uuid id
  }
  implicit class BlogPostIDOps(private val id: BlogPostID) extends AnyVal {
    def uuid: ju.UUID = BlogPostID uuid id
  }
}

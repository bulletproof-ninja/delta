
import scuff.FakeType
import java.{util => ju}
import java.util.UUID
import scala.util.Random

package airline {

  sealed abstract class OpaqueUUID
  extends FakeType[ju.UUID] {
    def toUUID(id: Type): ju.UUID
    def random(): Type
  }

  abstract class OpaqueShort
  extends FakeType[Short] {
    def toShort(id: Type): Short
    def random(): Type
  }

}

package object airline {
  type CustomerID = CustomerID.Type
  val CustomerID: OpaqueUUID = new OpaqueUUID {
    type Type = ju.UUID
    def apply(id: ju.UUID) = id
    def toUUID(id: Type) = id
    def random() = UUID.randomUUID()
  }

  type FlightID = FlightID.Type
  val FlightID: OpaqueUUID = new OpaqueUUID {
    type Type = ju.UUID
    def apply(id: ju.UUID) = id
    def toUUID(id: Type) = id
    def random() = UUID.randomUUID()
  }

  type PassengerID = PassengerID.Type
  val PassengerID: OpaqueShort = new OpaqueShort {
    type Type = Short
    def apply(id: Short) = id
    def toShort(id: Type) = id
    def random() = Random.nextInt().toShort
  }

  implicit def toUUID(id: OpaqueUUID#Type) = id.asInstanceOf[ju.UUID]
  implicit def toShort(id: OpaqueShort#Type) = id.asInstanceOf[Short]

  // implicit class OpaqueUUIDOps(private val id: OpaqueUUID#Type) extends AnyVal {
  //   def uuid: ju.UUID = id.asInstanceOf[ju.UUID]
  // }

}

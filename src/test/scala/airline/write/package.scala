package airline

import delta.write.{ Repository, MutableEntity }

package object write {

  implicit def newMetadata = new write.Metadata()

  type CustomerRepository = Repository[CustomerID, customer.Customer] with MutableEntity
  type FlightRepository = Repository[FlightID, flight.Flight] with MutableEntity
}

package airline.write.customer

import io.scalaland.chimney.dsl._
import delta.write.Entity

object Customer
extends Entity("customer", Projector) {

  type Id = airline.CustomerID

  type Type = Customer

  protected def init(id: Id, state: StateRef, concurrentUpdates: List[Transaction]) =
    new Customer(state)

  protected def StateRef(entity: Type) = entity.state

  protected def validate(data: state.Customer): Unit = {

  }

  def apply(cmd: RegisterCustomer): Customer = {
    val customer = new Customer
    customer.state apply
      cmd.into[CustomerRegistered]
        .transform
    customer
  }

}

class Customer private (private val state: Customer.StateRef = Customer.newStateRef()) {
  private def customer = state.get
}

package airline.write.customer

import io.scalaland.chimney.dsl._

object Projector
extends delta.Projector[state.Customer, CustomerEvent] {

  def init(evt: CustomerEvent) =
    evt dispatch new Dispatcher

  def next(cust: state.Customer, evt: CustomerEvent) =
    evt dispatch new Dispatcher(cust)

  private class Dispatcher(cust: state.Customer = null)
  extends CustomerEvents {
    type Return = state.Customer

    def handle(evt: CustomerRegistered): Return = {
      require(cust == null)
      evt.into[state.Customer]
        .transform

    }


  }

}

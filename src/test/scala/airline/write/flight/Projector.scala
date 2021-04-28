package airline.write.flight

import io.scalaland.chimney.dsl._

object Projector
extends delta.Projector[state.Flight, FlightEvent] {

  def init(evt: FlightEvent) =
    evt dispatch new Dispatcher

  def next(cust: state.Flight, evt: FlightEvent) =
    evt dispatch new Dispatcher(cust)

  private class Dispatcher(flight: state.Flight = null)
  extends FlightEvents {
    type Return = state.Flight

    def handle(evt: FlightScheduled): Return = {
      require(flight == null)
      evt.into[state.Flight]
        .transform
    }

  }

}

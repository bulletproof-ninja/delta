package airline.write.flight

import io.scalaland.chimney.dsl._

import delta.write.Entity

object Flight
extends Entity("flight", Projector) {

  type Id = airline.FlightID

  type Type = Flight

  protected def init(id: Id, state: StateRef, concurrentUpdates: List[Transaction]) =
    new Flight(state)

  protected def StateRef(entity: Type) = entity.state

  protected def validate(data: state.Flight): Unit = {

  }

  def apply(cmd: ScheduleFlight): Flight = {
    val flight = new Flight
    flight.state apply
      cmd.into[FlightScheduled]
        .transform
    flight
  }

}

class Flight(private val state: Flight.StateRef = Flight.newStateRef()) {
  private def flight = state.get
}

package airline.write.flight

import airline.write.RootEvent
import scala.{ SerialVersionUID => version }

sealed abstract class FlightEvent
extends RootEvent {
  type Callback = FlightEvents
}

trait FlightEvents {
  type Return

  def handle(evt: FlightScheduled): Return

}

@version(1)
final case class FlightScheduled()
extends FlightEvent { def dispatch(cb: Callback): cb.Return = cb handle this }

object FlightEvent {

  import scuff.json._, JsVal._

  trait JsonFormat
  extends FlightEvents {
    root: RootEvent.JsonFormat.type =>

    def handle(evt: FlightScheduled): Return =
      JsVal(evt).toJson

  }

}

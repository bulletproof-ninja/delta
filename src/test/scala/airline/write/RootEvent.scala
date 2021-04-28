package airline.write

import scuff.DoubleDispatch
import delta.util.ReflectiveDecoder
import delta.EventFormat
import scuff.serialVersionUID

abstract class RootEvent
extends DoubleDispatch

object RootEvent {

  type JSON = String

  object JsonFormat
  extends ReflectiveDecoder[RootEvent, JSON]
  with EventFormat[RootEvent, JSON]
  with customer.CustomerEvent.JsonFormat
  with flight.FlightEvent.JsonFormat {

    type Json = Encoded
    type Return = JSON

    protected def getName(cls: EventClass): String =
      cls.getSimpleName

    protected def getVersion(cls: EventClass): Byte =
      serialVersionUID(cls).asInstanceOf[Byte]

    def encode(evt: RootEvent): JSON =
      evt dispatch this.asInstanceOf[evt.Callback { type Return = JSON }]

}



}

package airline.write.customer

import airline.write.RootEvent
import scala.{ SerialVersionUID => version }

sealed abstract class CustomerEvent
extends RootEvent {
  type Callback = CustomerEvents
}

trait CustomerEvents {
  type Return

  def handle(evt: CustomerRegistered): Return

}

@version(1)
final case class CustomerRegistered(name: String)
extends CustomerEvent { def dispatch(cb: Callback): cb.Return = cb handle this }

object CustomerEvent {

  import scuff.json._, JsVal._

  trait JsonFormat
  extends CustomerEvents {
    root: RootEvent.JsonFormat.type =>

    def handle(evt: CustomerRegistered): Return = JsVal(evt).toJson
    def CustomerRegistered(json: Json): CustomerRegistered = {
      val ast = JsVal.parse(json.data).asObj
      json.version {
        case 1 => new CustomerRegistered(
          name = ast.name.asStr)
      }
    }

  }

}

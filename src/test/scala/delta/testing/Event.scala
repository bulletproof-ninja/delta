package delta.testing

sealed trait Event
object Event {
  case class Genesis(name: String, age: Int) extends Event
  case class NameChanged(newName: String) extends Event
  case class AgeChanged(newAge: Int) extends Event
}

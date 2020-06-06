import scuff.FakeType

package object delta {

  type Revision = Int // >= 0
  type Tick = Long

  type Channel = Channel.Type
  val Channel: FakeType[String] { type Type <: AnyRef } = new FakeType[String] {
    type Type = String
    def apply(str: String) = str
  }

}

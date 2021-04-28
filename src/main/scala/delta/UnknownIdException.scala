package delta

final case class UnknownIdException(id: Any)
extends RuntimeException(s"Unknown id: $id")

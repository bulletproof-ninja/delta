package blogging.write

import java.time.Instant

final case class Metadata(
  timestamp: Instant = Instant.now())
extends delta.write.Metadata {

  def toMap: Map[String,String] = Map(
    "timestamp" -> timestamp.toEpochMilli.toString
  )


}

object Metadata {
  def apply(map: Map[String, String]) =
    new Metadata(Instant.ofEpochMilli(map("timestamp").toLong))
}

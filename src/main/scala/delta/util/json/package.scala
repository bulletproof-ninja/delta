package delta.util

import scuff.Codec
import java.util.UUID

/** Some ad hoc JSON support. */
package object json {
  type JSON = String

  val JsonUUID = Codec[UUID, JSON](
    uuid => s""""$uuid"""",
    json => UUID fromString json.substring(1, json.length - 1))

}

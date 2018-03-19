package delta.jdbc.h2

import scuff._
import delta.jdbc.Dialect
import delta.jdbc.ColumnType

class H2Dialect[ID: ColumnType, EVT, SF: ColumnType](schema: Option[String])
    extends Dialect[ID, EVT, SF](schema) {

  def this(schema: String) = this(schema.optional)

  override protected def makeWHEREByChannelsOrEvents(chCount: Int, evtCount: Int): String = {
    val superWhere = super.makeWHEREByChannelsOrEvents(chCount, evtCount)
    if (evtCount == 0) superWhere
    else
      superWhere
        .replace(
          "(t.stream_id, t.revision) IN",
          "CONCAT(CAST(t.stream_id AS VARCHAR), ':' , CAST(t.revision AS VARCHAR)) IN")
        .replace(
          "SELECT e2.stream_id, e2.revision",
          "SELECT CONCAT(CAST(e2.stream_id AS VARCHAR), ':' , CAST(e2.revision AS VARCHAR))")
  }

}

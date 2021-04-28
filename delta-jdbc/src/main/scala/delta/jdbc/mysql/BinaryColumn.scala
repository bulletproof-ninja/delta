package delta.jdbc.mysql

import delta.jdbc.ColumnType
import java.sql.ResultSet

sealed abstract class BinaryColumn(
  val typeName: String)
extends ColumnType[Array[Byte]] {

  def readFrom(rs: ResultSet, col: Int): Array[Byte] =
    rs getBytes col

}

class BlobColumn256 extends BinaryColumn("TINYBLOB")
object BlobColumn256 extends BlobColumn256

class BlobColumn64K extends BinaryColumn("BLOB")
object BlobColumn64K extends BlobColumn64K

class BlobColumn16M extends BinaryColumn("MEDIUMBLOB")
object BlobColumn16M extends BlobColumn16M

// class BlobColumn4G extends BlobColumn("LONGBLOB")
// object BlobColumn4G extends BlobColumn4G

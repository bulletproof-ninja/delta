package delta

import java.util.UUID
import scala.reflect._
import java.math.BigInteger
import java.sql.ResultSet
import scuff.Numbers._
import java.io.ByteArrayInputStream
import java.sql.PreparedStatement

package jdbc {
  class VarBinaryColumn(len: String = "") extends ColumnType[Array[Byte]] {
    def this(maxLen: Int) = this(maxLen.toString)
    val typeName = len match {
      case "" => "VARBINARY"
      case _ => s"VARBINARY($len)"
    }
    def readFrom(row: ResultSet, col: Int) = row.getBytes(col)
  }
  class VarCharColumn(len: String = "") extends ColumnType[String] {
    def this(maxLen: Int) = this(maxLen.toString)
    val typeName = len match {
      case "" => "VARCHAR"
      case _ => s"VARCHAR($len)"
    }

    def readFrom(row: ResultSet, col: Int) = row.getString(col)
  }
  abstract class ScalaEnumColumn[EV <: Enumeration#Value: ClassTag](val enum: Enumeration)
    extends ColumnType[EV] with conv.ScalaEnumType[EV] {
    def typeName = "VARCHAR(255)"
    def readFrom(row: ResultSet, col: Int) = byName(row.getString(col))
  }

  object UUIDBinaryColumn extends ColumnType[UUID] {
    def typeName = "BINARY(16)"
    override def writeAs(uuid: UUID): Array[Byte] = {
      val bytes = new Array[Byte](16)
      longToBytes(uuid.getMostSignificantBits, bytes, 0)
      longToBytes(uuid.getLeastSignificantBits, bytes, 8)
    }
    def readFrom(row: ResultSet, col: Int) = {
      val bytes = row.getBytes(col)
      val msb = bytesToLong(bytes, 0)
      val lsb = bytesToLong(bytes, 8)
      new UUID(msb, lsb)
    }
  }
  object UUIDCharColumn extends ColumnType[UUID] {
    def typeName = "CHAR(36)"
    override def writeAs(uuid: UUID): String = uuid.toString
    def readFrom(row: ResultSet, col: Int) = UUID fromString row.getString(col)
  }

  object ClobColumn extends ColumnType[String] {
    def typeName = "CLOB"
    def readFrom(row: ResultSet, col: Int) = {
      val clob = row.getClob(col)
      clob.getSubString(1L, clob.length.toInt)
    }
  }
  object BlobColumn extends ColumnType[Array[Byte]] {
    def typeName = "BLOB"
    def readFrom(row: ResultSet, col: Int): Array[Byte] = {
      val blob = row.getBlob(col)
      blob.getBytes(1L, blob.length.toInt)
    }
    override def writeAs(bytes: Array[Byte]) = new ByteArrayInputStream(bytes)
  }

}

package object jdbc {
  implicit object LongColumn extends ColumnType[Long] {
    def typeName = "BIGINT"
    def readFrom(row: ResultSet, col: Int) = row.getLong(col)
  }
  implicit object IntColumn extends ColumnType[Int] {
    def typeName = "INT"
    def readFrom(row: ResultSet, col: Int) = row.getInt(col)
  }
  implicit object BigIntegerColumn extends ColumnType[BigInteger] {
    def typeName = "NUMERIC"
    def readFrom(row: ResultSet, col: Int): BigInteger = row.getBigDecimal(col).toBigInteger
    override def writeAs(bint: BigInteger) = new java.math.BigDecimal(bint)
  }
  implicit object BigIntColumn extends ColumnType[BigInt] {
    def typeName = BigIntegerColumn.typeName
    def readFrom(row: ResultSet, col: Int): BigInt = BigIntegerColumn.readFrom(row, col)
    override def writeAs(bint: BigInt) = BigIntegerColumn.writeAs(bint.underlying)
  }

  implicit object UnitColumn extends ColumnType[Unit] {
    private[this] final val Zero = java.lang.Byte.valueOf(0.asInstanceOf[Byte])
    def typeName = "TINYINT"
    def readFrom(row: ResultSet, col: Int): Unit = ()
    override def writeAs(unit: Unit) = Zero
  }
  implicit def JavaEnumColumn[T <: java.lang.Enum[T]: ClassTag] =
    new ColumnType[T] with conv.JavaEnumType[T] {
      def typeName = "VARCHAR(255)"
      def readFrom(row: ResultSet, col: Int) = byName(row.getString(col))
    }
  implicit def OptionColumn[T: ColumnType]: ColumnType[Option[T]] = new ColumnType[Option[T]] {
    def typeName = implicitly[ColumnType[T]].typeName
    def readFrom(row: ResultSet, col: Int): Option[T] = {
      val value = implicitly[ColumnType[T]].readFrom(row, col)
      if (row.wasNull || value == null) None
      else new Some(value)
    }
    override def writeAs(option: Option[T]) = option match {
      case Some(value) => implicitly[ColumnType[T]] writeAs value
      case None => null
    }
  }

  private[jdbc] implicit class DeltaPrep(private val ps: PreparedStatement) extends AnyVal {
    def setValue[T: ColumnType](colIdx: Int, value: T): Unit = ps.setObject(colIdx, implicitly[ColumnType[T]] writeAs value)
    def setChannel(colIdx: Int, ch: Transaction.Channel): Unit = ps.setString(colIdx, ch.toString)
  }
  private[jdbc] implicit class DeltaRes(private val rs: ResultSet) extends AnyVal {
    def getValue[T: ColumnType](colIdx: Int): T = implicitly[ColumnType[T]].readFrom(rs, colIdx)
    def getChannel(colIdx: Int): Transaction.Channel = Transaction.Channel(rs.getString(colIdx))
  }
}

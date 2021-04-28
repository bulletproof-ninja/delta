package delta

import java.util.UUID
import java.io.ByteArrayInputStream
import java.math.BigInteger

import scala.reflect._
import scala.util.Try

import java.sql.{ ResultSet, Connection, PreparedStatement, SQLException }

import scuff.Numbers._

package jdbc {

  object VarBinaryColumn extends VarBinaryColumn("") {
    def apply(maxLen: Int) = new VarBinaryColumn(maxLen)
    def apply(len: String) = new VarBinaryColumn(len)
  }
  class VarBinaryColumn(len: String = "") extends ColumnType[Array[Byte]] {
    def this(maxLen: Int) = this(maxLen.toString)
    val typeName = len match {
      case "" | null => "VARBINARY"
      case _ => s"VARBINARY($len)"
    }
    def readFrom(row: ResultSet, col: Int) = row.getBytes(col)
  }
  object CharColumn extends CharColumn("") {
    def apply(len: Int) = new VarCharColumn(len)
    def apply(len: String) = new VarCharColumn(len)
  }
  class CharColumn(len: String = "") extends ColumnType[String] {
    def this(len: Int) = this(len.toString)
    val typeName = len match {
      case "" | null => "CHAR"
      case _ => s"CHAR($len)"
    }
    def readFrom(row: ResultSet, col: Int) = row.getString(col)
  }
  object VarCharColumn extends VarCharColumn("") {
    def apply(maxLen: Int) = new VarCharColumn(maxLen)
    def apply(len: String) = new VarCharColumn(len)
  }
  class VarCharColumn(len: String = "") extends ColumnType[String] {
    def this(maxLen: Int) = this(maxLen.toString)
    val typeName = len match {
      case "" | null => "VARCHAR"
      case _ => s"VARCHAR($len)"
    }

    def readFrom(row: ResultSet, col: Int) = row.getString(col)
  }
  abstract class ScalaEnumColumn[EV <: Enumeration#Value: ClassTag](
    val enumeration: Enumeration)
  extends ColumnType[EV] with conv.ScalaEnumType[EV] {
    def typeName = "VARCHAR(255)"
    def readFrom(row: ResultSet, col: Int) = byName(row.getString(col))
  }

  object UUIDBinaryColumn extends UUIDBinaryColumn
  class UUIDBinaryColumn extends ColumnType[UUID] {
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

  object UUIDCharColumn extends UUIDCharColumn
  class UUIDCharColumn extends ColumnType[UUID] {
    def typeName = "CHAR(36)"
    override def writeAs(uuid: UUID): String = uuid.toString
    def readFrom(row: ResultSet, col: Int) = UUID fromString row.getString(col)
  }

  object JsonColumn extends JsonColumn
  class JsonColumn extends ColumnType[String] {
    def typeName = "JSON"
    def readFrom(row: ResultSet, col: Int) = row.getString(col)
  }

  object ClobColumn extends ClobColumn
  class ClobColumn extends ColumnType[String] {
    def typeName = "CLOB"
    def readFrom(row: ResultSet, col: Int) = {
      val clob = row.getClob(col)
      clob.getSubString(1L, clob.length.toInt)
    }
  }

  object BlobColumn extends BlobColumn
  class BlobColumn extends ColumnType[Array[Byte]] {
    def typeName = "BLOB"
    def readFrom(row: ResultSet, col: Int): Array[Byte] = {
      val blob = row.getBlob(col)
      blob.getBytes(1L, blob.length.toInt)
    }
    override def writeAs(bytes: Array[Byte]) = new ByteArrayInputStream(bytes)
  }

  object ByteColumn extends ByteColumn
  class ByteColumn extends ColumnType[Byte] {
    def typeName = "TINYINT"
    def readFrom(row: ResultSet, col: Int) = row.getByte(col)
  }

}

package object jdbc {

  implicit object BoolColumn extends ColumnType[Boolean] {
    def typeName = "BOOLEAN"
    def readFrom(row: ResultSet, col: Int) = row.getBoolean(col)
  }

  implicit object LongColumn extends ColumnType[Long] {
    def typeName = "BIGINT"
    def readFrom(row: ResultSet, col: Int) = row.getLong(col)
  }
  implicit object IntColumn extends ColumnType[Int] {
    def typeName = "INT"
    def readFrom(row: ResultSet, col: Int) = row.getInt(col)
  }
  implicit object ShortColumn extends ColumnType[Short] {
    def typeName = "SMALLINT"
    def readFrom(row: ResultSet, col: Int) = row.getShort(col)
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
    def typeName = "CHAR"
    def readFrom(row: ResultSet, col: Int): Unit = ()
    override def writeAs(unit: Unit) = "_"
  }
  implicit object NullColumn extends ColumnType[Null] {
    def typeName = "CHAR"
    def readFrom(row: ResultSet, col: Int): Null = null
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
      case None | Some(null) => null
      case Some(value) => implicitly[ColumnType[T]] writeAs value
    }
  }

  private[jdbc] implicit class DeltaConn(private val conn: Connection) extends AnyVal {

    @inline private def failed(action: String, sql: String, se: SQLException): SQLException =
      new SQLException(
        s"Failed (state:${se.getSQLState}, err:${se.getErrorCode}) to $action statement:\n$sql", se.getSQLState, se.getErrorCode, se)

    def prepare[R](sql: String)(thunk: PreparedStatement => R): R = {
      val ps = try conn prepareStatement sql catch {
        case cause: SQLException => throw failed("prepare", sql, cause)
      }
      try thunk(ps) catch {
        case cause: SQLException => throw failed("execute", sql, cause)
      } finally Try(ps.close)
    }
  }

  private[jdbc] implicit class DeltaPrep(private val ps: PreparedStatement) extends AnyVal {
    def setValue[T: ColumnType](colIdx: Int, value: T): Unit =
      ps.setObject(colIdx, implicitly[ColumnType[T]] writeAs value)
    def setChannel(colIdx: Int, ch: Channel): Unit =
      ps.setString(colIdx, ch.toString)
  }
  private[jdbc] implicit class DeltaRes(private val rs: ResultSet) extends AnyVal {
    def getValue[T: ReadColumn](colIdx: Int): T = implicitly[ReadColumn[T]].readFrom(rs, colIdx)
    def getChannel(colIdx: Int): Channel = Channel(rs.getString(colIdx))
  }

  implicit def QueryValue[T: ColumnType](multiple: Iterable[T]): QueryValue =
    multiple.toList match {
      case Nil => NOT_NULL
      case single :: Nil => EQUALS(single)
      case multiple => IN(multiple)
    }

  implicit def QueryValue[T: ColumnType](single: T): QueryValue =
    if (single == null) NULL
    else EQUALS(single)

  def QueryValue[T: ColumnType](first: T, second: T, others: T*): QueryValue =
    IN(first :: second :: others.toList)

}

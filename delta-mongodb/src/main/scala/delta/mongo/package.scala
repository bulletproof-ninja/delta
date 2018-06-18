package delta

import java.util.concurrent.ArrayBlockingQueue

import scala.concurrent.{ Future, Promise, TimeoutException }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.reflect.{ ClassTag }
import scala.util.{ Failure, Success, Try }

import org.bson.{ BsonReader, BsonWriter, UuidRepresentation }
import org.bson.codecs._

import com.mongodb.async.SingleResultCallback
import java.util.UUID
import org.bson._
import org.bson.types.Decimal128

package object mongo {

  import language.implicitConversions

  implicit def tuple2Codec[A: Codec, B: Codec] = new Codec[(A, B)] {
    def getEncoderClass = classOf[Tuple2[_, _]].asInstanceOf[Class[Tuple2[A, B]]]
    def encode(writer: BsonWriter, value: (A, B), encoderContext: EncoderContext): Unit = {
      writer.writeStartArray()
      implicitly[Codec[A]].encode(writer, value._1, encoderContext)
      implicitly[Codec[B]].encode(writer, value._2, encoderContext)
      writer.writeEndArray()
    }
    def decode(reader: BsonReader, decoderContext: DecoderContext): (A, B) = {
      reader.readStartArray()
      val a: A = implicitly[Codec[A]].decode(reader, decoderContext)
      val b: B = implicitly[Codec[B]].decode(reader, decoderContext)
      reader.readEndArray()
      a -> b
    }
  }
  implicit val objectIdCodec = new ObjectIdCodec
  implicit val uuidCodec: Codec[UUID] = new UuidCodec(UuidRepresentation.STANDARD)
  implicit val stringCodec: Codec[String] = new StringCodec
  implicit val intCodec = new IntegerCodec().asInstanceOf[Codec[Int]]
  implicit val longCodec = new LongCodec().asInstanceOf[Codec[Long]]
  implicit val unitCodec = new Codec[Unit] {
    def getEncoderClass = classOf[Unit]
    def encode(writer: BsonWriter, value: Unit, encoderContext: EncoderContext): Unit = {
      writer.writeUndefined()
    }
    def decode(reader: BsonReader, decoderContext: DecoderContext): Unit = {
      reader.readUndefined()
    }
  }
  implicit def JavaEnumCodec[E <: java.lang.Enum[E]: ClassTag] = new JavaEnumCodec[E]
  def ScalaEnumCodec[E <: Enumeration](enum: E): Codec[E#Value] = new Codec[E#Value] {
    val getEncoderClass = enum.values.head.getClass.asInstanceOf[Class[E#Value]]
    private[this] val byName = enum.values.foldLeft(Map.empty[String, E#Value]) {
      case (map, enum) => map.updated(enum.toString, enum)
    }
    def encode(writer: BsonWriter, value: E#Value, encoderContext: EncoderContext): Unit = {
      stringCodec.encode(writer, value.toString, encoderContext)
    }
    def decode(reader: BsonReader, decoderContext: DecoderContext): E#Value = {
      byName apply stringCodec.decode(reader, decoderContext)
    }
  }

  implicit def toBson(int: Int): BsonValue = new BsonInt32(int)
  implicit def toBson(long: Long): BsonValue = new BsonInt64(long)
  implicit def toBson(bool: Boolean): BsonValue = if (bool) BsonBoolean.TRUE else BsonBoolean.FALSE
  implicit def toBson(str: String): BsonValue = if (str == null) BsonNull.VALUE else new BsonString(str)
  implicit def toBson(dbl: Double): BsonValue = new BsonDouble(dbl)
  implicit def toBson(bd: BigDecimal): BsonValue = if (bd == null) BsonNull.VALUE else toBson(bd.underlying)
  implicit def toBson(bd: java.math.BigDecimal): BsonValue = if (bd == null) BsonNull.VALUE else new BsonDecimal128(new Decimal128(bd))
  implicit def toBson(seq: collection.Seq[BsonValue]): BsonValue = if (seq == null) BsonNull.VALUE else {
    import collection.JavaConverters._
    new BsonArray(seq.asJava)
  }

  def withFutureCallback[R](
      thunk: (=> SingleResultCallback[R]) => Unit): Future[Option[R]] = {
    val promise = Promise[Option[R]]
    var used = false
      def callback = if (!used) new SingleResultCallback[R] {
        used = true
        def onResult(result: R, t: Throwable): Unit = {
          if (t != null) promise failure t
          else promise success Option(result)
        }
      }
      else throw new IllegalStateException("Cannot use callback multiple times")
    thunk(callback)
    promise.future
  }

  def withBlockingCallback[R](
      timeout: FiniteDuration = 30.seconds)(
      thunk: (=> SingleResultCallback[R]) => Unit): Option[R] = {
    val queue = new ArrayBlockingQueue[Try[R]](1)
    var used = false
      def callback = if (!used) new SingleResultCallback[R] {
        used = true
        def onResult(result: R, t: Throwable): Unit = {
          if (t != null) queue offer Failure(t)
          else queue offer Success(result)
        }
      }
      else throw new IllegalStateException("Cannot use callback multiple times")
    thunk(callback)
    queue.poll(timeout.length, timeout.unit) match {
      case null => throw new TimeoutException("Timed out waiting for callback")
      case result => Option(result.get)
    }
  }

}

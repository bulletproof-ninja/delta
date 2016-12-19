package ulysses

import java.util.concurrent.ArrayBlockingQueue
import scala.concurrent.{ Future, Promise, TimeoutException }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.util.{ Failure, Success, Try }
import com.mongodb.async.SingleResultCallback
import org.bson.codecs.configuration.CodecRegistry
import scala.reflect._
import org.bson.Document
import org.bson.codecs.Codec
import com.mongodb.async.client._
import scala.collection.concurrent.TrieMap
import org.bson.types.ObjectId
import java.util.UUID
import org.bson.BsonWriter
import org.bson.BsonReader
import org.bson.codecs._
import org.bson.UuidRepresentation
import org.bson.types.Binary
import org.bson.BsonBinarySubType
import scuff.Numbers

package object mongo {

  implicit val ObjectIdCodec = new ObjectIdCodec
  implicit val UUIDCodec = new UuidCodec(UuidRepresentation.STANDARD)
  implicit val StringCodec = new StringCodec
  implicit val IntCodec = new IntegerCodec().asInstanceOf[Codec[Int]]
  implicit val LongCodec = new LongCodec().asInstanceOf[Codec[Long]]
  implicit val UnitCodec = new Codec[Unit] {
    def getEncoderClass = classOf[Unit]
    def encode(writer: BsonWriter, value: Unit, encoderContext: EncoderContext) {
      writer.writeUndefined()
    }
    def decode(reader: BsonReader, decoderContext: DecoderContext): Unit = {
      reader.readUndefined()
    }
  }
  implicit def JavaEnumCodec[E <: java.lang.Enum[E]: ClassTag] = new Codec[E] {
    val getEncoderClass = classTag[E].runtimeClass.asInstanceOf[Class[E]]
    private[this] val byName =
      getEncoderClass.getEnumConstants.foldLeft(Map.empty[String, E]) {
        case (map, enum: E) => map.updated(enum.name, enum)
      }
    def encode(writer: BsonWriter, value: E, encoderContext: EncoderContext) {
      StringCodec.encode(writer, value.name, encoderContext)
    }
    def decode(reader: BsonReader, decoderContext: DecoderContext): E = {
      byName apply StringCodec.decode(reader, decoderContext)
    }
  }

  def withFutureCallback[R](thunk: (=> SingleResultCallback[R]) => Unit): Future[Option[R]] = {
    val promise = Promise[Option[R]]
    var used = false
      def callback = if (!used) new SingleResultCallback[R] {
        used = true
        def onResult(result: R, t: Throwable) {
          if (t != null) promise failure t
          else promise success Option(result)
        }
      }
      else throw new IllegalStateException("Cannot use callback multiple times")
    thunk(callback)
    promise.future
  }

  def withBlockingCallback[R](timeout: FiniteDuration = 30.seconds)(thunk: (=> SingleResultCallback[R]) => Unit): Option[R] = {
    val queue = new ArrayBlockingQueue[Try[R]](1)
    var used = false
      def callback = if (!used) new SingleResultCallback[R] {
        used = true
        def onResult(result: R, t: Throwable) {
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

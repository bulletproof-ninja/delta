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
import org.bson.codecs.ObjectIdCodec
import org.bson.codecs.UuidCodec
import org.bson.UuidRepresentation
import org.bson.codecs.StringCodec

package object mongo {

  implicit val ObjectIdCodec = new ObjectIdCodec
  implicit val UUIDCodec = new UuidCodec(UuidRepresentation.STANDARD)
  implicit val StringCodec = new StringCodec
  implicit val IntCodec = new Codec[Int] {

  }
  implicit def JavaEnumCodec[T <: java.lang.Enum[T]: ClassTag] =
    new TypeCodec[T] with conv.JavaEnumType[T] {
      def readFrom(doc: Document, key: String) = byName(doc.getString(key))
    }
  abstract class ScalaEnumColumn[EV <: Enumeration#Value: ClassTag](val enum: Enumeration)
      extends TypeCodec[EV] with conv.ScalaEnumType[EV] {
    def readFrom(doc: Document, key: String) = byName(doc.getString(key))
  }

  private class UlyssesCodecRegistry(delegate: CodecRegistry) extends CodecRegistry {
    private[this] val map = new TrieMap[Class[_], Codec[_]]
    def get[T](cls: Class[T]): Codec[T] =
      map.get(cls).getOrElse(delegate.get(cls)).asInstanceOf[Codec[T]] match {
        case null => get(cls.getSuperclass.asInstanceOf[Class[T]])
        case codec => codec
      }
    def register(codec: Codec[_]): Unit = map.put(codec.getEncoderClass, codec)
  }

  implicit class UlyssesCollection[T](val coll: MongoCollection[T]) extends AnyVal {
    def withCodec(codec: Codec[_]): MongoCollection[T] = {
      val reg = coll.getCodecRegistry match {
        case reg: UlyssesCodecRegistry => reg
        case other => new UlyssesCodecRegistry(other)
      }
      reg.register(codec)
      coll.withCodecRegistry(reg)
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

  def withBlockingCallback[R](timeout: FiniteDuration = 10.seconds)(thunk: (=> SingleResultCallback[R]) => Unit): Option[R] = {
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

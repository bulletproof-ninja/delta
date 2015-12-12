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

package object mongo {

  private class UlyssesCodecRegistry(delegate: CodecRegistry) extends CodecRegistry {
    private[this] val map = new TrieMap[Class[_], Codec[_]]
    def get[T](cls: Class[T]): Codec[T] = map.get(cls).getOrElse(delegate.get(cls)).asInstanceOf[Codec[T]]
    def register[T: ClassTag](codec: Codec[T]): Unit = map.put(classTag[T].runtimeClass, codec)
  }

  implicit class UlyssesCollection[T](val coll: MongoCollection[T]) extends AnyVal {
    def withCodec[A: ClassTag](codec: Codec[A]): MongoCollection[T] = {
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

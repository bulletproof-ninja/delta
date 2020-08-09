package delta.hazelcast

import com.hazelcast.core.ExecutionCallback
import scala.concurrent.Promise

private[hazelcast] object CallbackPromise {
  def option[T]: CallbackPromise[T, Option[T]] = option[T] { Promise[Option[T]]() }
  def option[T](promise: Promise[Option[T]]): CallbackPromise[T, Option[T]] = new CallbackPromise[T, Option[T]](Option(_), promise)
  def apply[T, P](adapt: T => P, promise: Promise[P] = Promise[P]()): CallbackPromise[T, P] = new CallbackPromise(adapt, promise)
  def apply[P]: CallbackPromise[P, P] = new CallbackPromise(identity, Promise[P]())
  def apply[P](promise: Promise[P]): CallbackPromise[P, P] = new CallbackPromise(identity, promise)
}

private[hazelcast] class CallbackPromise[T, P] protected (adapt: T => P, promise: Promise[P] = Promise[P]())
  extends ExecutionCallback[T] {

  def future = promise.future
  def onResponse(value: T) = promise success adapt(value)
  def onFailure(th: Throwable) = promise failure th
}

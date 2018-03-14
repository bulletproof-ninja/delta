package delta.java

import scala.collection.JavaConverters._
import scala.collection.Map
import java.util.Optional
import scala.concurrent.{ Future, ExecutionContext }
import java.util.stream._
import java.util.Spliterators
import java.util.Spliterator
import delta.util.StreamProcessStore
import delta.Snapshot

object ScalaHelp {

  /** Unwrap state, returning `null` if empty. */
  def unwrap[T >: Null](opt: Option[T]): T = opt.orNull

  /** Turn Scala `Map` into Java `Map`. */
  def asJava[K, V](map: Map[K, V]): java.util.Map[K, V] = map.asJava
  /** Turn Scala `Option` into Java `Optional`. */
  def asJava[T >: Null](opt: Option[T]): Optional[T] = Optional.ofNullable(opt.orNull)

  /** Turn Java `Map` into Scala `Map`. */
  def asScala[K, V](map: java.util.Map[K, V]): Map[K, V] = map.asScala
  /** Turn Java `Iterable` into Scala `Iterable`. */
  def asScala[T](iter: java.lang.Iterable[T]): Iterable[T] = iter.asScala
  /** Turn Java `Optional` into Scala `Option`. */
  def asScala[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get) else None

  /**
    * Transpose `Stream` of `Future`s into `Future` of `Stream`s.
    */
  def transpose[T](futures: Stream[Future[T]], exeCtx: ExecutionContext): Future[Stream[T]] =
    transpose[T, T](futures, exeCtx, identity)
  /**
    * Transpose `Stream` of `Future`s into `Future` of `Stream`s,
    * while mapping the stream content to a different type.
    */
  def transpose[T, R](futures: Stream[Future[T]], exeCtx: ExecutionContext, map: T => R): Future[Stream[R]] = {
      implicit def ec = exeCtx
    (Future sequence futures.iterator.asScala).map { iter =>
      StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter.map(map).asJava, Spliterator.ORDERED), false)
    }
  }

  /**
    * Transpose `Stream` of `Future`s into `Future` of `Stream`s,
    * while reducing the stream content to a single instance.
    */
  def transposeAndReduce[T, R](
      futures: Stream[Future[T]], exeCtx: ExecutionContext,
      default: R, reducer: (R, T) => R): Future[R] = {
      implicit def ec = exeCtx
    (Future sequence futures.iterator.asScala).map { iter =>
      iter.foldLeft(default)(reducer)
    }
  }

  def writeToStore[K, V](snapshots: java.util.Map[K, Snapshot[V]], batchSize: Int, store: StreamProcessStore[K, V])(implicit ec: ExecutionContext): Future[Void] = {
    require(batchSize > 0, s"Must have positive batch size, not $batchSize")
    val futures: Iterator[Future[Unit]] =
      if (batchSize == 1) {
        snapshots.asScala.iterator.map {
          case (key, snapshot) => store.write(key, snapshot)
        }
      } else {
        snapshots.asScala.grouped(batchSize).map(store.writeBatch)
      }
    (Future sequence futures).map(_ => null: Void)
  }
}

package delta.java

import scala.jdk.CollectionConverters._
import scala.collection._
import java.util.Optional
import scala.concurrent.{ Future, ExecutionContext }
import java.util.stream._
import java.util.Spliterators
import java.util.Spliterator
import delta.process.StreamProcessStore
import delta.Snapshot
import java.util.function.Consumer
import scala.util.Failure
import scala.util.control.NoStackTrace
import scala.util.Try

object ScalaHelp {

  /** Unwrap state, returning `null` if empty. */
  def getOrNull[T >: Null](opt: Option[T]): T = opt.orNull

  /** Turn Scala `Map` into Java `Map`. */
  def asJava[K, V](map: Map[K, V]): java.util.Map[K, V] = map.asJava
  /** Turn Scala `Option` into Java `Optional`. */
  def asJava[T >: Null](opt: Option[T]): Optional[T] = Optional.ofNullable(opt.orNull)
  /** Turn Scala `Iterable` into Java `Iterable`. */
  def asJava[T](iter: Iterable[T]): java.lang.Iterable[T] = iter.asJava

  /** Turn Java `Map` into Scala immutable `Map`. */
  def asScala[K, V](map: java.util.Map[K, _ <: V]): immutable.Map[K, V] = map.asScala.toMap
  /** Turn Java `Iterable` into Scala `Iterable`. */
  def asScala[T](iter: java.lang.Iterable[T]): Iterable[T] = iter.asScala
  /** Turn Java `Optional` into Scala `Option`. */
  def asScala[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get) else None

  def tuple[A, B](a: A, b: B): (A, B) = a -> b
  def tuple[A, B, C](a: A, b: B, c: C): (A, B, C) = (a, b, c)

  def filter[T](opt: Option[_ >: T], tc: Class[T]): Option[T] = opt.filter(v => tc isInstance v).asInstanceOf[Option[T]]

  def Map[K, V]() = collection.immutable.Map.empty[K, V]
  def Map[K, V](key: K, value: V) = collection.immutable.Map(key -> value)

  /** var-args to Scala `Seq`. */
  @annotation.varargs
  def Seq[T](ts: T*): collection.immutable.Seq[T] = ts.toVector
  def Seq[T](stream: java.util.stream.Stream[T]): collection.immutable.Seq[T] =
    stream.toArray.toVector.asInstanceOf[collection.immutable.Seq[T]]

  /** Combine two `Future`s into single `Future`. */
  def combine[A, B](exeCtx: ExecutionContext, futureA: Future[A], futureB: Future[B]): Future[(A, B)] = {
      implicit def ec = exeCtx
    for {
      a <- futureA
      b <- futureB
    } yield (a, b)
  }
  /** Combine three `Future`s into single `Future`. */
  def combine[A, B, C](exeCtx: ExecutionContext, futureA: Future[A], futureB: Future[B], futureC: Future[C]): Future[(A, B, C)] = {
      implicit def ec = exeCtx
    for {
      a <- futureA
      b <- futureB
      c <- futureC
    } yield (a, b, c)
  }
  /** Combine four `Future`s into single `Future`. */
  def combine[A, B, C, D](exeCtx: ExecutionContext, futureA: Future[A], futureB: Future[B], futureC: Future[C], futureD: Future[D]): Future[(A, B, C, D)] = {
      implicit def ec = exeCtx
    for {
      a <- futureA
      b <- futureB
      c <- futureC
      d <- futureD
    } yield (a, b, c, d)
  }
  /** Combine five `Future`s into single `Future`. */
  def combine[A, B, C, D, E](exeCtx: ExecutionContext, futureA: Future[A], futureB: Future[B], futureC: Future[C], futureD: Future[D], futureE: Future[E]): Future[(A, B, C, D, E)] = {
      implicit def ec = exeCtx
    for {
      a <- futureA
      b <- futureB
      c <- futureC
      d <- futureD
      e <- futureE
    } yield (a, b, c, d, e)
  }
  /** Combine six `Future`s into single `Future`. */
  def combine[A, B, C, D, E, F](exeCtx: ExecutionContext, futureA: Future[A], futureB: Future[B], futureC: Future[C], futureD: Future[D], futureE: Future[E], futureF: Future[F]): Future[(A, B, C, D, E, F)] = {
      implicit def ec = exeCtx
    for {
      a <- futureA
      b <- futureB
      c <- futureC
      d <- futureD
      e <- futureE
      f <- futureF
    } yield (a, b, c, d, e, f)
  }
  /** Combine seven `Future`s into single `Future`. */
  def combine[A, B, C, D, E, F, G](exeCtx: ExecutionContext, futureA: Future[A], futureB: Future[B], futureC: Future[C], futureD: Future[D], futureE: Future[E], futureF: Future[F], futureG: Future[G]): Future[(A, B, C, D, E, F, G)] = {
      implicit def ec = exeCtx
    for {
      a <- futureA
      b <- futureB
      c <- futureC
      d <- futureD
      e <- futureE
      f <- futureF
      g <- futureG
    } yield (a, b, c, d, e, f, g)
  }

  /** Transpose `Stream` of `Future`s into `Future` of `Stream`s. */
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
      default: R, projector: (R, T) => R): Future[R] = {
      implicit def ec = exeCtx
    (Future sequence futures.iterator.asScala).map { iter =>
      iter.foldLeft(default)(projector)
    }
  }

  def writeToStore[K, S, U](
      snapshots: java.util.Map[K, Snapshot[S]],
      batchSize: Int,
      store: StreamProcessStore[K, S, U])(implicit ec: ExecutionContext): Future[Void] = {
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

  def adapt[T](report: Consumer[T]): T => Unit = t => report.accept(t)

  def reportFailure[T](errorMessage: String, failureReporter: Consumer[Throwable]): PartialFunction[Try[T], Unit] = {
    case Failure(th) => failureReporter accept new RuntimeException(errorMessage, th) with NoStackTrace
  }
}

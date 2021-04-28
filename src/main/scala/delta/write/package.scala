package delta

import scala.concurrent.Future

/**
  * Support for the write model.
  * @note Uses DDD concepts.
  */
package object write {
  implicit def toMetadata[MD <: Metadata](unit: Unit)(implicit md: MD): Future[MD] = Future successful md
  implicit def futureMetadata[MD <: Metadata](implicit md: MD): Future[MD] = Future successful md
  implicit def withMetadata[R, MD <: Metadata](r: R)(implicit md: MD): Future[(R, Metadata)] = Future successful (r -> md)
}

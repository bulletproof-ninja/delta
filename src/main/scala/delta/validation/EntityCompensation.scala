package delta.validation

import delta.write._

import scala.concurrent._
import scala.util.control.NonFatal

import scuff.concurrent.ScuffFutureObject

/**
  *
  * @tparam ID Identifier
  * @tparam S State
  * @tparam E Entity
  * @param repo Entity repository
  * @param validations Validations
  * @param metadata Repository metadata function
  */
class EntityCompensation[SID, S, ID, E](
  repo: EntityRepository[SID, _, _, ID, E],
  validations: List[Validation[ID, S, E]])(
  implicit
  metadata: () => Metadata,
  idConv: SID => ID)
extends Compensation[SID, S] {

  def this(
    repo: EntityRepository[SID, _, _, ID, E],
    first: Validation[ID, S, E], others: Validation[ID, S, E]*)(
    implicit
    metadata: () => Metadata,
    idConv: SID => ID) = this(repo, first :: others.toList)

  private[this] val singleValidation = validations match {
    case single :: Nil => single
    case _ => null
  }

  def ifNeeded(stream: SID, tick: Tick, state: S)(
      implicit
      ec: ExecutionContext): Future[Outcome] = {

    val id: ID = stream
    if (singleValidation != null) singleValidation(singleValidation, id, tick, state)
    else (Future sequence validations.map(_.validate(id, tick, state)))
      .map(_.flatten)
      .flatMap {
        case Nil => Future_NotNeeded
        case compensations => compensate(id, compensations)
      }
    }

  /** Slightly faster validation. */
  private def singleValidation(
      validation: Validation[ID, S, E],
      id: ID, tick: Tick, state: S)(
      implicit
      ec: ExecutionContext): Future[Outcome] =
    validation.validate(id, tick, state) flatMap {
      case None => Future_NotNeeded
      case Some(compensation) =>
        compensate(id, compensation :: Nil)
    }

  private def compensate(
      id: ID, compensations: List[Compensate[E]])(
      implicit
      ec: ExecutionContext): Future[Outcome] =
    repo.update(id) {
      case (entity, _) => try {
        compensations.foreach {
          _ apply entity
        }
        Future.unit
      } catch {
        case NonFatal(cause) => Future failed cause
      }
    }(metadata()) map {
      case (_, revision) => Compensated(revision)
    }

}

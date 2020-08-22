package delta.validation

import delta.write._

import scala.concurrent._
import scala.util.{ Try, Success, Failure }
import scala.util.control.NonFatal
import scuff.Codec

/**
  * General entity [[delta.validation.Compensation]] that issues
  * compensation for a specific entity type.
  *
  * @tparam SID Stream identifier
  * @tparam S Stream state type
  * @tparam ID Entity identifier
  * @tparam E Entity type
  * @param idCodec The two-way conversion codec for stream <=> entity ids
  * @param repo Entity repository
  * @param validator Required validator
  * @param moreValidators Optional additional validators
  * @param metadata Repository metadata function
  */
class EntityCompensation[SID, S, ID, E](
  idCodec: Codec[SID, ID],
  repo: Repository[ID, E] with MutableEntity,
  validator: Validator[ID, S, E], moreValidators: Validator[ID, S, E]*)(
  implicit
  metadata: () => Metadata)
extends Compensation[SID, S] {

  @inline
  implicit private def sid2id(sid: SID): ID = idCodec encode sid

  private[this] val validators = validator :: moreValidators.toList
  def ifNeeded(stream: SID, snapshot: Snapshot)(
      implicit
      ec: ExecutionContext): Future[Map[SID, Try[Revision]]] =

    if (moreValidators.isEmpty) {
      singleValidation(validator, stream, snapshot)
    } else {
      (Future sequence validators.map(_.validate(stream, snapshot)))
        .map(_.flatten)
        .flatMap {
          case Nil => Future_EmptyMap
          case compensations => compensate(compensations.toMap)
        }
    }

  /** Slightly faster validation. */
  private def singleValidation(
      validator: Validator[ID, S, E],
      stream: SID, snapshot: Snapshot)(
      implicit
      ec: ExecutionContext): Future[Map[SID, Try[Revision]]] =
    validator.validate(stream, snapshot) flatMap {
      case empty if empty.isEmpty => Future_EmptyMap
      case compensations =>
        compensate(compensations)
    }

  private def compensate(compensations: Map[ID, Compensate[E]])(
      implicit
      ec: ExecutionContext): Future[Map[SID, Try[Revision]]] = try {

    val futureUpdates: Iterable[(SID, Future[Revision])] =
      compensations.map {
        case (id, compensation) =>
          val updateFuture: Future[Revision] =
            repo.update(id) {
              case (entity, _) =>
                Future fromTry Try {
                  compensation(entity)
                }
            }(metadata()) map {
              case (_, revision) => revision
            }
          idCodec.decode(id) -> updateFuture
        }

    Future.traverse(futureUpdates) {
      case (id, futureRevision) =>
        futureRevision map {
          case revision => id -> Success(revision)
        } recover {
          case NonFatal(cause) => id -> Failure(new CompensationFailure(id, cause))
        }
    }.map(_.toMap)

  } catch {
    case NonFatal(cause) => Future failed cause
  }

}

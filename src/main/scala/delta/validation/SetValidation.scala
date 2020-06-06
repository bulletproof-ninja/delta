package delta.validation

import scala.concurrent._

import scuff._, concurrent._
import scala.annotation.tailrec

/**
  * Enforce Set semantics, i.e. disallow duplicates,
  * and compensate if detected.
  * @param indexLookup
  * @param txProcessor
  * @tparam ID Id type
  * @tparam S state type
  * @tparam Ctx Compensation context
  */
trait SetValidation[ID, S, Ctx]
extends Validation[ID, S, Ctx] {

  protected type WinMetric
  /** Query for values. Should never return empty map. */
  protected def query(value: UniqueType): Future[Map[ID, WinMetric]]
  /** Pick winner. Will never be empty map. */
  protected def pickWinner(candidates: Map[ID, WinMetric]): ID

  protected type UniqueType
  protected def compensate(duplicate: UniqueType, context: Ctx): Unit
  protected def valuesFrom(state: S): List[UniqueType]

  @tailrec
  private def compensate(remove: List[UniqueType], context: Ctx): Unit =
    remove match {
      case Nil => // Done
      case duplicate :: remaining =>
        compensate(duplicate, context)
        compensate(remaining, context)
    }

  def validate(
      id: ID, tick: Tick, state: S)(
      implicit
      validationContext: ExecutionContext)
      : Future[Option[Compensate[Ctx]]] =
    valuesFrom(state) match {
      case Nil => Future.none
      case forVerification =>
        val forCompensation: List[Future[Option[UniqueType]]] =
          forVerification.map { value =>
            query(value)
              .map { candidates =>
                if (candidates.isEmpty)
                  sys.error(s"Cannot perform set validation, as `$value` has no matches!")
                else if (pickWinner(candidates) == id)
                  None
                else
                  Some(value)
              }
          }
        (Future sequence forCompensation).map {
          case None :: Nil => None
          case options =>
            options.flatten match {
              case Nil => None
              case nonEmpty => Some {
                compensate(nonEmpty, _)
              }
            }
        }
    }

}

object BySeniority {
  /**
    * Get oldest entry, determined by lowest `Tick` value.
    * @param candidates Map of candidates.
    * @return Oldest key
    */
  def oldest[K](candidates: collection.Map[K, delta.Tick]): Option[K] =
    if (candidates.isEmpty) None
    else Some(candidates.toList.sortBy(_._2).head._1)

}
/**
  * If duplicates are found, the oldest entry wins,
  * leading to a compensating action for the current
  * invalid state.
  */
trait BySeniority[ID, S, Ctx] {
  validation: SetValidation[ID, S, Ctx] =>

  protected type WinMetric = Tick
  protected def pickWinner(candidates: Map[ID, Tick]): ID =
    try BySeniority.oldest(candidates).get catch {
      case _: NoSuchElementException => sys.error("Cannot pick winner from empty map!")
    }

}

/** Convenience trait; less repetition. */
trait SetValidationBySeniority[ID, S, Ctx]
extends SetValidation[ID, S, Ctx]
with BySeniority[ID, S, Ctx]

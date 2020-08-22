package delta.validation

import scala.concurrent._
import scala.collection.compat._
import scala.util.control.NonFatal

/**
  * Partial [[delta.validation.Validator]] implementation
  * that enforces a certain global uniqueness constraint.
  * The process is broken down into the discrete steps:
  *
  * 1. Determine if anything needs validation by implementing `def needValidation`.
  * 2. If step 1 returns non-empty, try to globally find any matches to the qualifiers by implementing `def findMatches`.
  * 3. If any matches are found, we have a conflict, a uniqueness violation, which is resolved by implementing `def pickWinner`.
  * 4. When the winner is found, `def compensate` is called for the losers, which is expected to execute compensating actions through the supplied context.
  *
  * @tparam ID Identifier
  * @tparam S State
  * @tparam Ctx Compensation context
  */
trait GlobalSetValidator[ID, S, Ctx]
extends Validator[ID, S, Ctx] {

  /** Validation qualifier. */
  protected type Qualifier

  /**
    * @return List of qualifiers that need validation.
    * For efficient processing, don't return already validated.
    */
  protected def needValidation(state: S): Set[Qualifier]

  /** Win/lose metric. */
  protected type Metric
  protected def toMetric(snapshot: Snapshot): Metric

  protected def findMatches(q: Qualifier): Future[Map[ID, Metric]]

  /** Pick winner. Will never receive empty map. */
  protected def pickWinner(candidates: Map[ID, Metric]): ID

  protected def compensate(q: Qualifier, validatedState: S, context: Ctx): Unit

  private def compensateAll(
      qualifiers: Set[Qualifier], validatedState: S, context: Ctx)(
      implicit
      failureReporter: ExecutionContext): Unit = {

    val qs = qualifiers.iterator
    while (qs.hasNext) try {
      compensate(qs.next(), validatedState, context)
    } catch {
      case NonFatal(cause) => failureReporter reportFailure cause
    }
  }

  /** Validate and return compensation function, if needed. */
  def validate(
      id: ID, snapshot: Snapshot)(
      implicit
      validationContext: ExecutionContext)
      : Future[Map[ID, Compensate[Ctx]]] = {

    val qualifiers = needValidation(snapshot.state)
    if (qualifiers.isEmpty) Future_EmptyMap
    else {
      val losers: Iterable[Future[(Qualifier, Map[ID, Metric])]] =
        qualifiers.map { q =>
          findMatches(q)
            .map { existing =>
              if (existing.isEmpty) q -> existing
              else {
                val candidates = existing.updated(id, toMetric(snapshot))
                val winner = pickWinner(candidates)
                q -> (candidates - winner)
              }
            }
        }

      val futureQualifiers: Future[Map[ID, Set[Qualifier]]] =
        (Future sequence losers) map {
          case losers =>
            losers
              .iterator
              .filter(_._2.nonEmpty)
              .foldLeft(Map.empty[ID, Set[Qualifier]]) {
                case (qualifiers, (qualifier, metrics)) =>
                  qualifiers ++ metrics.map {
                    case (id, _) => id -> (qualifiers.getOrElse(id, Set.empty) + qualifier)
                  }
              }
        }

      futureQualifiers map {
        _.view.mapValues { qualifiers =>
          compensateAll(qualifiers, snapshot.state, _)
        }.toMap
      }

    }

  }

}

/**
  * If duplicates are found, the oldest entry (by tick) wins,
  * leading to a compensating action for the current invalid stream.
  *
  * @tparam ID Identifier
  * @tparam S State
  * @tparam Ctx Compensation context
  */
trait BySeniority[ID, S, Ctx] {
  validation: GlobalSetValidator[ID, S, Ctx] =>

  protected type Metric = Tick
  protected def toMetric(snapshot: Snapshot) = snapshot.tick
  protected def pickWinner(candidates: Map[ID, Tick]): ID = pickOldestNonEmpty(candidates)

}

/**
  * Global set validation, where the oldest entry, by tick,
  * is determined as winner.
  *
  * Just a convenience trait for less repetition.
  *
  * @tparam ID Identifier
  * @tparam S State
  * @tparam Ctx Compensation context
  */
trait SetValidationBySeniority[ID, S, Ctx]
extends GlobalSetValidator[ID, S, Ctx]
with BySeniority[ID, S, Ctx]

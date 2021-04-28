package delta.process

import scala.collection.compat._

import scala.reflect.ClassTag

object JoinState {

  final class Enrichment[SID, +T: ClassTag] private[JoinState] (
    val streams: Set[SID],
    initState: => T,
    enrichState: T => T) {

    import scuff._
    def applyOption[S >: T](state: Option[S]): S = enrichState(state.collectAs[T] getOrElse initState)
    // def apply[S >: T](state: S): S = enrichState(Option(state).collectAs[T] getOrElse initState)
    def apply[S >: T](state: S): S =
      state match {
        case state: T => enrichState(state)
        case _ => state
      }

  }
  object Enrichment {
    private[this] val _empty = new Enrichment[Any, Any](Set.empty, ???, _ => ???)
    def empty[SID, S] = _empty.asInstanceOf[Enrichment[SID, S]]

  /**
    * @param stream The stream identifier
    * @param enrichState Idempotent enrichment function
    */
    def apply[ID, SID, S: ClassTag](
        stream: ID,
        initState: => S)(
        enrichState: S => S)(
        implicit
        idConv: ID => SID) =
      new JoinState.Enrichment[SID, S](Set(stream), initState, enrichState)
  /**
    * @param streams The stream identifiers
    * @param enrichState Idempotent enrichment function
    */
    def apply[ID, SID, S: ClassTag](
        streams: Iterable[ID],
        initState: => S)(
        enrichState: S => S)(
        implicit
        idConv: ID => SID) =
      new JoinState.Enrichment[SID, S](streams.map(idConv).toSet, initState, enrichState)

  }

}

/**
 * Build additional, or exclusively, collateral
 * state, similar to an outer join.
 * @note Because the state is not built from its
 * own stream, there's no guarantee of ordering
 * other than the monotonic order of the stream
 * itself. Duplicate events *may* be observed and
 * causal ordering cannot be expected (in fact
 * is unlikely), thus processing should be
 * forgiving in that regard. This means that if
 * absolute accuracy is important, consumption
 * must be idempotent. This also means that data
 * set size becomes an important consideration.
 */
trait JoinState[SID, EVT, S >: Null] {
  proc: TransactionProcessor[SID, EVT, S] =>

  protected type Enrichment = JoinState.Enrichment[SID, S]
  protected def Enrichment = JoinState.Enrichment
  protected type EnrichmentEvaluator = PartialFunction[EVT, Enrichment]

  /**
    * Evaluate individual events for enrichment of _other_
    * streams.
    * @note The enricment function must be idempotent, as it
    * might get invoked multiple times under certain scenarios
    * (e.g. partial failure).
    * @param refTx Current transaction, for reference
    * @param refState State from current transaction, for reference.
    * Note: The provided transaction has been applied, so the state
    * already has the event applied that is being evaluated. That means
    * that if the event itself is insufficient to enrich the other stream's
    * state, the reference state can be used.
    * @return Partial function identifying events that is used to enrich
    * other streams than the current.
    * Note: The partial function can also be complete, instead returning
    * an empty `Enrichment`.
    */
  protected def EnrichmentEvaluator(
      refTx: Transaction, refState: S)
      : EnrichmentEvaluator

  protected def evaluateEnrichments(
      tx: Transaction, state: S): List[Enrichment] = {

    val evaluator = EnrichmentEvaluator(tx, state)

    val enrichments: Iterator[Enrichment] =
      tx.events.iterator
        .asInstanceOf[Iterator[EVT]]
        .filter(evaluator.isDefinedAt)
        .map(evaluator.apply)
        .filter(_.streams.nonEmpty)
        .tapEach { enrichment =>
          if (enrichment.streams contains tx.stream) throw new IllegalStateException(
            s"Self-enrichment for stream ${tx.stream} is nonsensical. Use the regular `process(Transaction, Option[S]): S` method instead.")
        }

    enrichments.toList

  }

  protected def groupEnrichments(
      enrichments: List[Enrichment])
      : Map[SID, Option[S] => S] =
    enrichments
      .foldLeft(Map.empty[SID, Option[S] => S]) {
        case (map, enrich) =>
          enrich.streams.foldLeft(map) {
            case (map, stream) =>
              // FIXME: Faster, for 2.13+
              // map.updatedWith(stream) {
              //   case None => Some(enrich.applyOption)
              //   case Some(priorEnrichment) => Some {
              //     priorEnrichment andThen enrich.apply
              //   }
              // }
              map.get(stream) match {
                case None =>
                  map.updated(stream, enrich.applyOption _)
                case Some(priorEnrichment) =>
                  map.updated(stream, priorEnrichment andThen enrich.apply)
              }
          }
    }

}

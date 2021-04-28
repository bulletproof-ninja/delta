package delta.process

import delta.Snapshot

import scala.concurrent._

/**
  * Monotonic processor for primary stream, *NOT*
  * for the joined streams.
  * Please read notes: [[delta.process.JoinState]]
  */
trait MonotonicJoinProcessor[SID, EVT, S >: Null, U]
extends MonotonicProcessor[SID, EVT, S, U]
with JoinState[SID, EVT, S] {

  protected def processEnrichments(
      enrichments: List[Enrichment],
      tx: Transaction, txState: S)(
      implicit
      ec: ExecutionContext): Future[S] = {
    val futureUpdates: Iterable[Future[Unit]] =
      groupEnrichments(enrichments).map {
        case (id, enrichState) =>
          implicit val ec = processContext(id)
          val result: Future[(Option[delta.process.Update[U]], _)] =
            this.processStore.upsert(id) { optSnapshot =>
              val (revision, tick) = optSnapshot.map { snapshot =>
                snapshot.revision -> (snapshot.tick max tx.tick)
              }.getOrElse(-1 -> tx.tick)
              val enrichedState = enrichState(optSnapshot.map(_.state))
              val enrichedSnapshot = Snapshot(enrichedState, revision, tick)
              val unit = ()
              Future successful Some(enrichedSnapshot) -> unit
            }
          result.map {
            case (Some(update), _) => onUpdate(id, update)
            case _ => // No change
          }
      }
      Future.sequence(futureUpdates)
        .map(_ => txState)
  }

  protected final abstract override def process(tx: Transaction, streamState: Option[S]): Future[S] = {
    implicit val ec = processContext(tx.stream)
    super.process(tx, streamState)
      .flatMap { updState =>
        val enrichments = evaluateEnrichments(tx, updState)
        processEnrichments(enrichments, tx, updState)
      }
  }

}

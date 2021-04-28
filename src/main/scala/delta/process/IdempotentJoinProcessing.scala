package delta.process

/**
  * Extension of [[delta.process.IdempotentProcessing]] with support
  * for join state (cross stream state).
  * @see [[delta.process.IdempotentProcessing]] for details.
  */
abstract class IdempotentJoinProcessing[SID, EVT, S >: Null, U]
extends IdempotentProcessing[SID, EVT, S, U]
with JoinState[SID, EVT, S] {

  override protected def replayProcessor(es: EventSource, config: ReplayProcessConfig) = {
    final class ReplayJoinProc
    extends ReplayProc(config, adHocExeCtx)
    with MonotonicJoinProcessor[SID, EVT, S, U] {

      protected def EnrichmentEvaluator(
          refTx: Transaction, refState: S)
          : EnrichmentEvaluator =
        IdempotentJoinProcessing.this.EnrichmentEvaluator(refTx, refState)

    }
    new ReplayJoinProc
  }

  override protected def liveProcessor(es: EventSource, config: LiveProcessConfig) = {
    final class LiveJoinProc
    extends LiveProc(es, config)
    with MonotonicJoinProcessor[SID, EVT, S, U] {

      protected def EnrichmentEvaluator(
          refTx: Transaction, refState: S)
          : EnrichmentEvaluator =
        IdempotentJoinProcessing.this.EnrichmentEvaluator(refTx, refState)

    }
    new LiveJoinProc
  }

}

/**
  * Recommended super class for implementing [[delta.EventSource]]
  * consumption with cross referenced streams in different channels.
  * @see [[delta.process.IdempotentJoinProcessing]] for details.
  */
abstract class IdempotentJoinConsumer[SID, EVT, InUse >: Null, U]
extends IdempotentJoinProcessing[SID, EVT, InUse, U]
with EventSourceConsumer[SID, EVT]

abstract class SimpleIdempotentJoinConsumer[SID, EVT, S >: Null]
extends IdempotentJoinConsumer[SID, EVT, S, S]

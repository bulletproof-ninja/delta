package delta.process

import scala.reflect.ClassTag

/**
  * Recommended super class for implementing [[delta.EventSource]]
  * consumption.
  * @see [[delta.process.IdempotentProcessing]] for details.
  * @tparam SID Stream identifier type
  * @tparam EVT Event type
  * @tparam InUse The state type used for event transformation
  * @tparam U The update type. Is often the same as `InUse`, unless a smaller diff type is desired.
  */
abstract class IdempotentConsumer[SID, EVT: ClassTag, InUse >: Null, U]
extends IdempotentProcessing[SID, EVT, InUse, U]
with EventSourceConsumer[SID, EVT]

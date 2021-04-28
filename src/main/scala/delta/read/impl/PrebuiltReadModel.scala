package delta.read.impl

import scala.concurrent._, duration._

import delta.read._
import java.util.concurrent.ScheduledExecutorService

/**
 * Read model that relies on some externally built store
 * that is continuously updated by ''another thread or process''.
 * The subscription implementation is left out, but can be
 * easily augmented by adding [[delta.read.MessageHubSupport]] or
 * [[delta.read.MessageTransportSupport]] for greater flexibility.
 * @tparam ID The specific identifier type
 * @tparam S The stream state type
 * @tparam SID The more general stream identifier
 * @tparam U The update type
 */
private[impl] abstract class BasePrebuiltReadModel[ID, SID, S, U](
  val name: String,
  protected val defaultReadTimeout: FiniteDuration = DefaultReadTimeout)(
  implicit
  idConv: ID => SID,
  protected val scheduler: ScheduledExecutorService)
extends ReadModel[ID, S]
with SubscriptionSupport[ID, S, U] {

  protected type StreamId = SID
  protected def StreamId(id: ID) = idConv(id)

}

abstract class PrebuiltReadModel[ID, SID, S](
  name: String,
  defaultReadTimeout: FiniteDuration = DefaultReadTimeout)(
  implicit
  idConv: ID => SID,
  scheduler: ScheduledExecutorService)
extends BasePrebuiltReadModel[ID, SID, S, S](name, defaultReadTimeout) {

  protected def updateState(id: ID, prevState: Option[S], currState: S) = Some(currState)

}

abstract class FlexPrebuiltReadModel[ID, SID, S, U](
  name: String,
  defaultReadTimeout: FiniteDuration = DefaultReadTimeout)(
  implicit
  idConv: ID => SID,
  scheduler: ScheduledExecutorService)
extends BasePrebuiltReadModel[ID, SID, S, U](name, defaultReadTimeout)

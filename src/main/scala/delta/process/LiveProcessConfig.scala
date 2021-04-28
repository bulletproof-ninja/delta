package delta.process

import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.ScheduledExecutorService

/**
  * Live process configuration.
  *
  * @param onMissingRevision
  * How to handle missing transactions. It's
  * generally adviced to use some delay, to avoid
  * replaying transactions that are simply out of order.
  */
final case class LiveProcessConfig(
  onMissingRevision: LiveProcessConfig.OnMissingRevision)

object LiveProcessConfig {

  sealed abstract class OnMissingRevision

  object OnMissingRevision {

    def apply(
        delay: FiniteDuration)(
        implicit
        scheduler: ScheduledExecutorService)
        : OnMissingRevision = {

      if (delay.length == 0) ImmediateReplay
      else DelayedReplay(delay, scheduler)
    }

  }

  case object Ignore
  extends OnMissingRevision

  case object ImmediateReplay
  extends OnMissingRevision

  /**
  * @param delay Delay before replaying missing revisions.
  * This allows some margin for delayed out-of-order transactions,
  * either due to choice of messaging infrastructure, or consistency
  * validation, where transaction propagation is delayed and
  * intentionally out-of-order during a consistency violation.
  * @param scheduler Used to schedule delayed replay of missing transactions.
  */
  case class DelayedReplay(delay: FiniteDuration, scheduler: ScheduledExecutorService)
  extends OnMissingRevision {
    require(delay.length > 0, s"Must have positive delay: $delay")
  }

}

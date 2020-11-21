package delta.process

import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.ScheduledExecutorService

final case class LiveProcessConfig(
  /**
    * Time delay before replaying missing revisions.
    * This allows some margin for delayed out-of-order transactions,
    * either due to choice of messaging infrastructure, or consistency
    * validation, where transaction propagation is delayed and
    * intentionally out-of-order during a consistency violation.
    */
  replayMissingDelay: FiniteDuration,
  /**
    * Scheduler to handle replay of missing transactions.
    */
  replayMissingScheduler: ScheduledExecutorService)

package delta.hazelcast

import com.hazelcast.core.IExecutorService
import com.hazelcast.logging.ILogger
import scala.concurrent.ExecutionContext

class HzPartitionedExecutionContext(
  exe: IExecutorService,
  logger: ILogger,
  partitionKey: Option[Any] = None)
extends ExecutionContext {

  def this(exe: IExecutorService, logger: ILogger, partitionKey: Any) =
    this(exe, logger, Option(partitionKey))

  def reportFailure(cause: Throwable): Unit = logger severe cause

  def execute(runnable: Runnable): Unit = {
    val key = partitionKey getOrElse runnable.hashCode()
    exe.executeOnKeyOwner(runnable, key)
  }

  def onKeyOwner(partitionKey: Any): ExecutionContext =
    if (this.partitionKey contains partitionKey) this
    else new HzPartitionedExecutionContext(exe, logger, partitionKey)

}

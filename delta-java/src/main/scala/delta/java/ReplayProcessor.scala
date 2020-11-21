package delta.java

import scala.concurrent.Future
import delta.process.ReplayCompletion

trait ReplayProcessor[ID, EVT]
  extends scuff.StreamConsumer[delta.Transaction[ID, _ >: EVT], Future[ReplayCompletion[ID]]]

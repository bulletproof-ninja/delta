package delta.java

import scala.concurrent.Future

trait ReplayProcessor[ID, EVT]
  extends scuff.StreamConsumer[delta.Transaction[ID, _ >: EVT], Future[Object]]

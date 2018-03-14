package delta.java

import scala.concurrent.Future

trait BatchProcessor[ID, EVT]
  extends scuff.StreamConsumer[delta.Transaction[ID, _ >: EVT, _], Future[Object]]

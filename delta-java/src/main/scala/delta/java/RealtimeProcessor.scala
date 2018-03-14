package delta.java

import delta.Transaction
import scala.concurrent.Future

trait RealtimeProcessor[ID, EVT]
  extends (Transaction[ID, _ >: EVT, _] => Future[Unit])

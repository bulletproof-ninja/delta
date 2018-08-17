package delta.java

import delta.Transaction
import scala.concurrent.Future

trait LiveProcessor[ID, EVT]
  extends (Transaction[ID, _ >: EVT] => Future[Unit])

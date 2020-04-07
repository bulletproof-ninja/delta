package delta.java

import scala.jdk.CollectionConverters._

abstract class Entity[ID, T, S >: Null, EVT](name: String, projector: delta.Projector[S, EVT])
  extends delta.write.Entity[S, EVT](name, projector) {

  type Id = ID
  type Type = Object

  protected final def init(state: State, mergeEvents: List[EVT]): Type = init(state, mergeEvents.asJava).asInstanceOf[Type]
  protected final def state(entity: Object): State = getState(entity.asInstanceOf[T])

  protected def init(state: delta.write.State[S, EVT], mergeEvents: java.util.List[Event]): T
  protected def getState(entity: T): delta.write.State[S, EVT]

}

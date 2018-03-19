package delta.java

import collection.JavaConverters._

abstract class Entity[ID, T, S >: Null, EVT](name: String, reducer: delta.EventReducer[S, EVT])
  extends delta.ddd.Entity[S, EVT](name, reducer) {

  type Id = ID
  type Type = Object

  protected final def init(state: State, mergeEvents: List[EVT]): Type = init(state, mergeEvents.asJava).asInstanceOf[Type]
  protected final def state(entity: Object): State = getState(entity.asInstanceOf[T])

  protected def init(state: delta.ddd.State[S, EVT], mergeEvents: java.util.List[Event]): T
  protected def getState(entity: T): delta.ddd.State[S, EVT]

}

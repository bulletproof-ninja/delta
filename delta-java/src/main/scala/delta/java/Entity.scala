package delta.java

import scala.jdk.CollectionConverters._

abstract class Entity[ID, T, S >: Null, EVT](name: String, projector: delta.Projector[S, EVT])
  extends delta.write.Entity[S, EVT](name, projector) {

  type Id = ID
  type Type = Object

  protected final def init(
      state: StateRef, concurrentUpdates: List[Transaction]): Type =
    init(state, concurrentUpdates.asJava).asInstanceOf[Type]

  protected final def StateRef(entity: Object) = getState(entity.asInstanceOf[T])

  protected def init(state: delta.write.StateRef[S, EVT], concurrentUpdates: java.util.List[Transaction]): T
  protected def getState(entity: T): delta.write.StateRef[S, EVT]

}

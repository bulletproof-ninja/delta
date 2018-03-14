package delta.java

import collection.JavaConverters._

abstract class Entity[ID, Type, S >: Null, EVT](reducer: delta.EventReducer[S, EVT])
  extends delta.ddd.Entity[Type, S, EVT](reducer) {

  type Id = ID

  protected final def init(state: State, mergeEvents: List[EVT]): Type = {
    val iterable = new java.lang.Iterable[EVT] {
      def iterator = mergeEvents.iterator.asJava
    }
    init(state, iterable)
  }

  protected def init(state: State, mergeEvents: java.lang.Iterable[EVT]): Type

}

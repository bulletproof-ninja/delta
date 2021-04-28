package delta.java

import java.util.{ Collection, Collections }
import java.util.function.{ BiFunction, Supplier }

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import delta.process.JoinState
import delta.process.ReplayProcessConfig
import delta.process.StreamProcessStore


/** Monotonic processor. */
abstract class MonotonicProcessor[ID, EVT, S >: Null, U](
  process: BiFunction[delta.Transaction[ID, _ >: EVT], Option[S], S],
  protected val processStore: StreamProcessStore[ID, S, U])
extends delta.process.MonotonicProcessor[ID, EVT, S, U]
with LiveProcessor[ID, EVT] {
  protected def process(tx: Transaction, currState: Option[S]) =
    this.process.apply(tx, currState)
}

/** Monotonic replay processor. */
abstract class MonotonicReplayProcessor[ID, EVT, S >: Null, U](
  process: BiFunction[delta.Transaction[ID, _ >: EVT], Option[S], S],
  protected val processStore: StreamProcessStore[ID, S, U],
  replayConfig: ReplayProcessConfig)
extends delta.process.MonotonicReplayProcessor[ID, EVT, S, U](replayConfig)
with ReplayProcessor[ID, EVT] {
  protected def process(tx: Transaction, currState: Option[S]) =
    this.process.apply(tx, currState)
}

/** Monotonic processor with join state. */
abstract class JoinStateProcessor[ID, EVT, S >: Null, U](
  process: BiFunction[delta.Transaction[ID, _ >: EVT], Option[S], S],
  processStore: StreamProcessStore[ID, S, U])
extends MonotonicProcessor[ID, EVT, S, U](process, processStore)
with delta.process.MonotonicJoinProcessor[ID, EVT, S, U]

/** Monotonic replay processor with join state. */
abstract class JoinStateReplayProcessor[ID, EVT, S >: Null, U](
  stateClass: Class[_ <: S],
  process: BiFunction[delta.Transaction[ID, _ >: EVT], Option[S], S],
  processStore: StreamProcessStore[ID, S, U],
  replayConfig: ReplayProcessConfig)
extends MonotonicReplayProcessor[ID, EVT, S, U](
  process, processStore, replayConfig)
with delta.process.MonotonicJoinProcessor[ID, EVT, S, U] {

  implicit private val stateTag = ClassTag[S](stateClass)

  protected final def Enrichment(
      stream: ID,
      newState: Supplier[S],
      enrich: java.util.function.Function[S, S])
      : Enrichment =
    Enrichment(Collections.singletonList(stream), newState, enrich)


  protected final def Enrichment(
      streams: Collection[ID],
      newState: Supplier[S],
      enrich: java.util.function.Function[S, S])
      : Enrichment =
    JoinState.Enrichment(streams.asScala, newState.get)(enrich.apply)

}

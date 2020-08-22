package delta.hazelcast.serializers

import java.io.{ ObjectInputStream, ObjectOutputStream }

import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }
import com.hazelcast.nio.serialization.StreamSerializer
import com.hazelcast.projection.Projection

import delta.{ Snapshot, Transaction, Projector }
import delta.process.{ Update, AsyncCodec }
import delta.hazelcast._

import scala.collection.immutable.TreeMap

trait TransactionSerializer
extends StreamSerializer[delta.Transaction[Any, Any]] {

  type Transaction = delta.Transaction[Any, Any]

  @inline private def serializer = delta.Transaction.Serialization

  def write(out: ObjectDataOutput, tx: Transaction): Unit = {
    val output = out match {
      case out: java.io.ObjectOutput => out
      case out: java.io.OutputStream => new ObjectOutputStream(out)
    }
    serializer.writeObject(tx, output)
  }

  def read(inp: ObjectDataInput): Transaction = {
    val input = inp match {
      case inp: java.io.ObjectInput => inp
      case inp: java.io.InputStream => new ObjectInputStream(inp)
    }
    serializer.readObject[Any, Any](input) {
      case (tick, ch, id, rev, metadata, events) =>
        new Transaction(tick, ch, id, rev, metadata, events)
    }
  }
}

trait SnapshotSerializer
extends StreamSerializer[Snapshot[Any]] {

  def write(out: ObjectDataOutput, s: Snapshot[Any]): Unit = {
    out writeObject s.state
    out writeInt s.revision
    out writeLong s.tick
  }

  def read(inp: ObjectDataInput) = {
    new delta.Snapshot(
      state = inp.readObject[Any],
      revision = inp.readInt,
      tick = inp.readLong)
  }
}

trait UpdateSerializer
extends StreamSerializer[Update[Any]] {

  def write(out: ObjectDataOutput, u: Update[Any]): Unit = {
    out writeObject u.changed.orNull
    out writeInt u.revision
    out writeLong u.tick
  }
  def read(inp: ObjectDataInput) =
    new Update(Option(inp.readObject[Any]), inp.readInt, inp.readLong)

}

trait DistributedMonotonicProcessorSerializer
extends StreamSerializer[DistributedMonotonicProcessor[Any, Any, Any, Any]] {

  def write(out: ObjectDataOutput, ep: DistributedMonotonicProcessor[Any, Any, Any, Any]): Unit = {
    out writeObject ep.tx
    out writeObject ep.projector
    out writeObject ep.stateCodec
    out writeUTF ep.getExecutorName
  }

  def read(inp: ObjectDataInput) = {
    val tx = inp.readObject[delta.Transaction[Any, Any]]
    val projector = inp.readObject[Projector[Any, Any]]
    val codec = inp.readObject[AsyncCodec[Any, Any]]
    val execName = inp.readUTF
    new delta.hazelcast.DistributedMonotonicProcessor(codec, execName, tx, projector)
  }

}

trait SnapshotUpdaterSerializer
extends StreamSerializer[delta.hazelcast.SnapshotUpdater[Any, Any]] {

  type Updater = delta.hazelcast.SnapshotUpdater[Any, Any]

  def write(out: ObjectDataOutput, ep: Updater): Unit = {
    out writeObject ep.update
  }

  def read(inp: ObjectDataInput): Updater = {
    new Updater(inp.readObject[Either[(Int, Long), Snapshot[Any]]])
  }
}

trait EntryStateSnapshotReaderSerializer
extends StreamSerializer[delta.hazelcast.EntryStateSnapshotReader[Any, Any]] {

  type Reader = delta.hazelcast.EntryStateSnapshotReader[Any, Any]

  def write(out: ObjectDataOutput, ep: Reader): Unit = {
    out writeObject ep.stateConv
  }
  def read(inp: ObjectDataInput): Reader = {
    val stateConv = inp.readObject[Any => Any]
    new Reader(stateConv)
  }

}

trait EntryUpdateResultSerializer
extends StreamSerializer[delta.hazelcast.EntryUpdateResult] {

  def write(out: ObjectDataOutput, res: delta.hazelcast.EntryUpdateResult): Unit = res match {
    case Updated(update) =>
      out writeByte 0
      out writeObject update
    case IgnoredDuplicate =>
      out writeByte 1
    case MissingRevisions(range) =>
      out writeByte 2
      out writeInt range.start
      out writeInt range.last
  }
  def read(inp: ObjectDataInput): delta.hazelcast.EntryUpdateResult = inp.readByte match {
    case 0 =>
      new Updated(inp.readObject[delta.process.Update[Any]])
    case 1 =>
      IgnoredDuplicate
    case 2 =>
      new MissingRevisions(inp.readInt to inp.readInt)
  }
}

trait EntryStateSerializer
extends StreamSerializer[EntryState[Any, Any]] {
  def write(out: ObjectDataOutput, es: EntryState[Any, Any]) = {
    out writeObject es.snapshot
    out writeBoolean es.contentUpdated
    out writeObject es.unapplied
  }
  def read(inp: ObjectDataInput): EntryState[Any, Any] = {
    val snapshot = inp.readObject[Snapshot[Any]]
    val contentUpdated= inp.readBoolean()
    val unapplied = inp.readObject[TreeMap[Int, Transaction[Any, Any]]]
    new EntryState[Any, Any](snapshot, contentUpdated, unapplied)
  }
}

trait StreamProcessStoreUpdaterSerializer
extends StreamSerializer[IMapStreamProcessStore.Updater[Any, Any]] {
  type Updater = IMapStreamProcessStore.Updater[Any, Any]
  def write(out: ObjectDataOutput, updater: Updater): Unit = updater match {
    case IMapStreamProcessStore.WriteReplacement(rev, tick, snapshot) =>
      out writeByte 0
      out writeInt rev
      out writeLong tick
      out writeObject snapshot
    case IMapStreamProcessStore.WriteIfAbsent(snapshot) =>
      out writeByte 1
      out writeObject snapshot
  }
  def read(inp: ObjectDataInput): Updater = {
    inp.readByte match {
      case 0 =>
        val rev = inp.readInt
        val tick = inp.readLong
        val snapshot = inp.readObject[Snapshot[Any]]
        new IMapStreamProcessStore.WriteReplacement[Any, Any](rev, tick, snapshot)
      case 1 =>
        val snapshot = inp.readObject[Snapshot[Any]]
        new IMapStreamProcessStore.WriteIfAbsent[Any, Any](snapshot)
    }
  }
}

trait ConcurrentMapStoreValueSerializer
extends StreamSerializer[delta.process.ConcurrentMapStore.State[Any]] {
  type State = delta.process.ConcurrentMapStore.State[Any]
  def write(out: ObjectDataOutput, value: State): Unit = {
    out writeBoolean value.modified
    out writeObject value.snapshot.state
    out writeInt value.snapshot.revision
    out writeLong value.snapshot.tick
  }
  def read(inp: ObjectDataInput): State = {
    val modified = inp.readBoolean()
    val snapshot = new Snapshot(inp.readObject[Any], inp.readInt, inp.readLong)
    new State(snapshot, modified)
  }

}

trait KeySnapshotProjectionSerializer
extends StreamSerializer[KeySnapshotProjection.type] {
  def write(out: ObjectDataOutput, value: KeySnapshotProjection.type): Unit = ()
  def read(inp: ObjectDataInput): KeySnapshotProjection.type = KeySnapshotProjection
}

trait KeyTickProjectionSerializer
extends StreamSerializer[KeyTickProjection.type] {
  def write(out: ObjectDataOutput, value: KeyTickProjection.type): Unit = ()
  def read(inp: ObjectDataInput): KeyTickProjection.type = KeyTickProjection
}

trait IMapDuplicateAggregatorSerializer
extends StreamSerializer[IMapDuplicateAggregator[Any, Any, Any]] {
  def write(out: ObjectDataOutput, value: IMapDuplicateAggregator[Any,Any,Any]): Unit = {
    out writeObject value.projection
  }
  def read(inp: ObjectDataInput): IMapDuplicateAggregator[Any,Any,Any] = {
    val projection = inp.readObject[Projection[Any, Any]]
    new IMapDuplicateAggregator(projection)
  }
}

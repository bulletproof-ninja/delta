package delta.hazelcast.serializers

import java.io.{ ObjectInputStream, ObjectOutputStream }
import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }
import com.hazelcast.nio.serialization.StreamSerializer
import delta.Projector
import delta.Snapshot
import delta.hazelcast.IgnoredDuplicate
import delta.hazelcast.MissingRevisions
import delta.hazelcast.Updated
import scala.reflect.ClassTag
import delta.hazelcast.EntryState
import scala.collection.immutable.TreeMap
import delta.Transaction

trait TransactionSerializer
  extends StreamSerializer[delta.Transaction[Any, Any]] {

  type TXN = delta.Transaction[Any, Any]

  @inline private def serializer = delta.Transaction.Serialization

  def write(out: ObjectDataOutput, txn: TXN): Unit = {
    val output = out match {
      case out: java.io.ObjectOutput => out
      case out: java.io.OutputStream => new ObjectOutputStream(out)
    }
    serializer.writeObject(txn, output)
  }

  def read(inp: ObjectDataInput): TXN = {
    val input = inp match {
      case inp: java.io.ObjectInput => inp
      case inp: java.io.InputStream => new ObjectInputStream(inp)
    }
    serializer.readObject[Any, Any](input) {
      case (tick, ch, id, rev, metadata, events) =>
        new TXN(tick, ch, id, rev, metadata, events)
    }
  }
}

trait SnapshotSerializer
  extends StreamSerializer[Snapshot[Any]] {

  def write(out: ObjectDataOutput, s: Snapshot[Any]): Unit = {
    out writeObject s.content
    out writeInt s.revision
    out writeLong s.tick
  }

  def read(inp: ObjectDataInput) = {
    new delta.Snapshot(
      content = inp.readObject[Any],
      revision = inp.readInt,
      tick = inp.readLong)
  }
}

trait DistributedMonotonicProcessorSerializer
  extends StreamSerializer[delta.hazelcast.DistributedMonotonicProcessor[Any, Any, Any]] {

  def write(out: ObjectDataOutput, ep: delta.hazelcast.DistributedMonotonicProcessor[Any, Any, Any]): Unit = {
    out writeObject ep.txn
    out writeObject ep.projector
    out writeObject ep.evtTag.runtimeClass
    out writeObject ep.stateTag.runtimeClass
  }

  def read(inp: ObjectDataInput) = {
    val txn = inp.readObject[delta.Transaction[Any, Any]]
    val projector = inp.readObject[Projector[Any, Any]]
    val evtTag = ClassTag[Any](inp.readObject[Class[Any]])
    val stateTag = ClassTag[Any](inp.readObject[Class[Any]])
    new delta.hazelcast.DistributedMonotonicProcessor(txn, projector)(evtTag, stateTag)
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
  extends StreamSerializer[delta.hazelcast.EntryStateSnapshotReader.type] {

  type Reader = delta.hazelcast.EntryStateSnapshotReader.type

  def write(out: ObjectDataOutput, ep: Reader): Unit = ()
  def read(inp: ObjectDataInput): Reader = delta.hazelcast.EntryStateSnapshotReader
}

trait EntryUpdateResultSerializer
  extends StreamSerializer[delta.hazelcast.EntryUpdateResult] {

  def write(out: ObjectDataOutput, res: delta.hazelcast.EntryUpdateResult): Unit = res match {
    case Updated(model) =>
      out writeByte 0
      out writeObject model
    case IgnoredDuplicate =>
      out writeByte 1
    case MissingRevisions(range) =>
      out writeByte 2
      out writeInt range.start
      out writeInt range.last
  }
  def read(inp: ObjectDataInput): delta.hazelcast.EntryUpdateResult = inp.readByte match {
    case 0 =>
      new Updated(inp.readObject[delta.Snapshot[Any]])
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

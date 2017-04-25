package delta.hazelcast.serializers

import java.io.{ ObjectInputStream, ObjectOutputStream }
import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }
import com.hazelcast.nio.serialization.StreamSerializer
import delta.Fold
import delta.Snapshot
import delta.hazelcast.IgnoredDuplicate
import delta.hazelcast.MissingRevisions
import delta.hazelcast.Updated

trait TransactionSerializer
    extends StreamSerializer[delta.Transaction[Any, Any, Any]] {

  type TXN = delta.Transaction[Any, Any, Any]

  def write(out: ObjectDataOutput, txn: TXN): Unit = {
    val output = out match {
      case out: java.io.ObjectOutput => out
      case out: java.io.OutputStream => new ObjectOutputStream(out)
    }
    delta.Transaction.writeObject(txn, output)
  }

  def read(inp: ObjectDataInput): TXN = {
    val input = inp match {
      case inp: java.io.ObjectInput => inp
      case inp: java.io.InputStream => new ObjectInputStream(inp)
    }
    delta.Transaction.readObject(input) {
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

trait TransactionProcessorSerializer
    extends StreamSerializer[delta.hazelcast.TransactionProcessor[Any, Any, Any]] {

  def write(out: ObjectDataOutput, ep: delta.hazelcast.TransactionProcessor[Any, Any, Any]): Unit = {
    out writeObject ep.txn
    out writeObject ep.stateFold
  }

  def read(inp: ObjectDataInput) = {
    val txn = inp.readObject[delta.Transaction[Any, Any, Any]]
    val fold = inp.readObject[Fold[Any, Any]]
    new delta.hazelcast.TransactionProcessor(txn, fold)
  }
}

trait SnapshotUpdaterSerializer
    extends StreamSerializer[delta.hazelcast.SnapshotUpdater[Any, Any]] {

  type Updater = delta.hazelcast.SnapshotUpdater[Any, Any]

  def write(out: ObjectDataOutput, ep: Updater): Unit = {
    out writeObject ep.update
  }

  def read(inp: ObjectDataInput): Updater = {
    inp.readObject[Either[(Int, Long), Snapshot[Any]]] match {
      case Right(snapshot) => new Updater(snapshot)
      case Left((rev, tick)) => new Updater(rev, tick)
    }

  }
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
      out writeInt range.end
  }
  def read(inp: ObjectDataInput): delta.hazelcast.EntryUpdateResult = inp.readByte match {
    case 0 =>
      new Updated(inp.readObject[delta.Snapshot[Any]])
    case 1 =>
      IgnoredDuplicate
    case 2 =>
      new MissingRevisions(new Range(inp.readInt, inp.readInt, 1))
  }
}

package delta.hazelcast.serializers

import java.io.{ ObjectInputStream, ObjectOutputStream }
import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }
import com.hazelcast.nio.serialization.StreamSerializer
import delta.ddd.Fold
import delta.hazelcast.IgnoredDuplicate
import delta.hazelcast.MissingRevisions
import delta.hazelcast.Updated

trait Transaction
    extends StreamSerializer[delta.Transaction[Any, Any, Any]] {

  type TXN = delta.Transaction[Any, Any, Any]

  def write(out: ObjectDataOutput, txn: TXN): Unit = {
    delta.Transaction.writeObject(txn, out.asInstanceOf[ObjectOutputStream])
  }

  def read(inp: ObjectDataInput): TXN = {
    delta.Transaction.readObject(inp.asInstanceOf[ObjectInputStream]) {
      case (tick, ch, id, rev, metadata, events) =>
        new TXN(tick, ch, id, rev, metadata, events)
    }
  }
}

trait ReadModel
    extends StreamSerializer[delta.cqrs.ReadModel[Any]] {

  def write(out: ObjectDataOutput, rm: delta.cqrs.ReadModel[Any]): Unit = {
    out writeObject rm.data
    out writeInt rm.revision
    out writeLong rm.tick
  }

  def read(inp: ObjectDataInput) = {
    new delta.cqrs.ReadModel(
      data = inp.readObject[Any],
      revision = inp.readInt,
      tick = inp.readLong)
  }
}

trait ReadModelUpdater
    extends StreamSerializer[delta.hazelcast.ReadModelUpdater[Any, Any, Any]] {

  def write(out: ObjectDataOutput, upd: delta.hazelcast.ReadModelUpdater[Any, Any, Any]): Unit = {
    out writeObject upd.txn
    out writeObject upd.modelUpdater
  }

  def read(inp: ObjectDataInput) = {
    val txn = inp.readObject[delta.Transaction[Any, Any, Any]]
    val updater = inp.readObject[Fold[Any, Any]]
    new delta.hazelcast.ReadModelUpdater(txn, updater)
  }
}

trait ModelUpdateResult
    extends StreamSerializer[delta.hazelcast.ModelUpdateResult] {

  def write(out: ObjectDataOutput, res: delta.hazelcast.ModelUpdateResult): Unit = res match {
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
  def read(inp: ObjectDataInput): delta.hazelcast.ModelUpdateResult = inp.readByte match {
    case 0 =>
      new Updated(inp.readObject[delta.cqrs.ReadModel[Any]])
    case 1 =>
      IgnoredDuplicate
    case 2 =>
      new MissingRevisions(new Range(inp.readInt, inp.readInt, 1))
  }
}

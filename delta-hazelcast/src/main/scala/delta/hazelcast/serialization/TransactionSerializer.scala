package delta.hazelcast.serialization

import java.io.{ ObjectInputStream, ObjectOutputStream }
import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }
import com.hazelcast.nio.serialization.StreamSerializer
import delta.Transaction

trait TransactionSerializer[ID, EVT, CH]
  extends StreamSerializer[Transaction[ID, EVT, CH]] {

  type TXN = Transaction[ID, EVT, CH]

  def write(out: ObjectDataOutput, txn: TXN): Unit = {
    Transaction.writeObject(txn, out.asInstanceOf[ObjectOutputStream])
  }

  def read(inp: ObjectDataInput): TXN = {
    Transaction.readObject(inp.asInstanceOf[ObjectInputStream]) {
      case (tick, ch, id, rev, metadata, events) =>
        new TXN(tick, ch, id, rev, metadata, events)
    }
  }
}

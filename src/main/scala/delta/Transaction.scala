package delta

import scala.util.control.NonFatal

@SerialVersionUID(1)
final case class Transaction[+ID, +EVT](
    tick: Tick,
    channel: Channel,
    stream: ID,
    revision: Revision,
    metadata: Map[String, String],
    events: List[EVT]) {
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    Transaction.Serialization.writeObject(this, out)
  }
  private def readObject(inp: java.io.ObjectInputStream): Unit = {
    Transaction.Serialization.readObject[ID, EVT](inp) {
      case (tick, ch, id, rev, metadata, events) =>
        val tx = new scuff.reflect.Surgeon(this)
        tx.tick = tick
        tx.channel = ch
        tx.stream = id
        tx.revision = rev
        tx.metadata = metadata
        tx.events = events
        this
    }
  }

}

object Transaction {

  object Serialization {
    def writeObject(tx: Transaction[_, _], out: java.io.ObjectOutput): Unit = {
      out.writeLong(tx.tick)
      out.writeUTF(tx.channel.toString)
      out.writeObject(tx.stream)
      out.writeInt(tx.revision)
      out.writeChar(tx.metadata.size)
      tx.metadata.foreach {
        case (key, value) =>
          out.writeUTF(key)
          out.writeUTF(value)
      }
      tx.events.reverse.foreach { evt =>
        try out writeObject evt catch {
          case NonFatal(cause) =>
            throw new RuntimeException(s"Failed to serialize event: ${evt.getClass.getName}", cause)
        }
      }
      out.writeObject(null)
    }
    @annotation.tailrec
    private def readEvents(
        in: java.io.ObjectInput,
        events: List[_ <: AnyRef] = Nil): List[_ <: AnyRef] =
      in.readObject match {
        case null => events
        case evt => readEvents(in, evt :: events)
      }
    def readObject[ID, EVT](
        inp: java.io.ObjectInput)(
        ctor: (Tick, Channel, ID, Revision, Map[String, String], List[EVT]) => Transaction[ID, EVT]): Transaction[ID, EVT] = {
      val tick = inp.readLong()
      val ch = Channel(inp.readUTF)
      val id = inp.readObject.asInstanceOf[ID]
      val rev = inp.readInt()
      val mdSize: Int = inp.readChar()
      var metadata = Map.empty[String, String]
      while (metadata.size < mdSize) {
        metadata += inp.readUTF -> inp.readUTF
      }
      val events = readEvents(inp).asInstanceOf[List[EVT]]
      ctor(tick, ch, id, rev, metadata, events)
    }
  }
}

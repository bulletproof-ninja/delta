package delta

@SerialVersionUID(1)
final case class Transaction[ID, EVT, CH](
    tick: Long,
    channel: CH,
    stream: ID,
    revision: Int,
    metadata: Map[String, String],
    events: List[EVT]) {
  private def writeObject(out: java.io.ObjectOutputStream) {
    Transaction.writeObject(this, out)
  }
  private def readObject(inp: java.io.ObjectInputStream) {
    Transaction.readObject[ID, EVT, CH](inp) {
      case (tick, ch, id, rev, metadata, events) =>
        val surgeon = new scuff.reflect.Surgeon(this)
        surgeon.set('tick, tick)
        surgeon.set('channel, ch)
        surgeon.set('stream, id)
        surgeon.set('revision, rev)
        surgeon.set('metadata, metadata)
        surgeon.set('events, events)
        this
    }
  }

}

object Transaction {
  def writeObject(txn: Transaction[_, _, _], out: java.io.ObjectOutputStream): Unit = {
    out.writeLong(txn.tick)
    out.writeObject(txn.channel)
    out.writeObject(txn.stream)
    out.writeInt(txn.revision)
    out.writeChar(txn.metadata.size)
    txn.metadata.foreach {
      case (key, value) =>
        out.writeUTF(key)
        out.writeUTF(value)
    }
    txn.events.reverse.foreach(out.writeObject)
    out.writeObject(null)
  }
  @annotation.tailrec
  private def readEvents(
    in: java.io.ObjectInputStream,
    events: List[_ <: AnyRef] = Nil): List[_ <: AnyRef] =
    in.readObject match {
      case null => events
      case evt => readEvents(in, evt :: events)
    }
  def readObject[ID, EVT, CH](
    inp: java.io.ObjectInputStream)(
      ctor: (Long, CH, ID, Int, Map[String, String], List[EVT]) => Transaction[ID, EVT, CH]): Transaction[ID, EVT, CH] = {
    val tick = inp.readLong()
    val ch = inp.readObject.asInstanceOf[CH]
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

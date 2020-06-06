package delta.redis

import java.io.{ InputStream, OutputStream }

import delta.Snapshot
import scuff.{ Serializer, StreamingSerializer }

class SnapshotSerializer[T](ser: Serializer[T])
    extends StreamingSerializer[Snapshot[T]] {
  def encodeInto(out: OutputStream)(s: Snapshot[T]) = asDataOutput(out) { out =>
    out.writeInt(s.revision)
    out.writeLong(s.tick)
    val content = ser encode s.state
    out.writeInt(content.length)
    out.write(content)
  }
  def decodeFrom(inp: InputStream): Snapshot[T] = asDataInput(inp) { inp =>
    val rev = inp.readInt()
    val tick = inp.readLong()
    val length = inp.readInt()
    val contentArr = new Array[Byte](length)
    inp.readFully(contentArr)
    val content = ser decode contentArr
    new Snapshot(content, rev, tick)
  }

}

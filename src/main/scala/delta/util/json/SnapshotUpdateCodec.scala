package delta.util.json

import scuff.Codec
import delta.Snapshot
import delta.process.SnapshotUpdate
import scuff.json._

object SnapshotUpdateCodec {
  def apply[T](snapshotJsonCodec: Codec[Snapshot[T], String]): Codec[SnapshotUpdate[T], String] =
    new SnapshotUpdateCodec(snapshotJsonCodec)

  def apply[T](contentJsonCodec: Codec[T, String], contentFieldName: String): Codec[SnapshotUpdate[T], String] =
    new SnapshotUpdateCodec(new SnapshotCodec(contentJsonCodec, contentFieldName))

}

/**
 * @tparam ID Id type
 * @tparam T Snapshot content type
 */
class SnapshotUpdateCodec[T](snapshotJsonCodec: Codec[Snapshot[T], String])
  extends Codec[SnapshotUpdate[T], String] {

  def this(contentJsonCodec: Codec[T, String], contentFieldName: String) =
    this(new SnapshotCodec(contentJsonCodec, contentFieldName))

  protected def contentUpdatedField: String = "contentUpdated"

  def encode(update: SnapshotUpdate[T]): String = {
    val snapshotJson = new java.lang.StringBuilder(snapshotJsonCodec.encode(update.snapshot).trim)
    snapshotJson.charAt(0) match {
      case '{' => snapshotJson.insert(1, s""""$contentUpdatedField":${update.contentUpdated},""")
      case '[' => snapshotJson.insert(1, s"""${update.contentUpdated},""")
      case _ => sys.error(s"Cannot work with snapshot encoding: $snapshotJson")
    }
    snapshotJson.toString
  }

  def decode(json: String): SnapshotUpdate[T] = {
    val ast = (JsVal parse json)
    val (contentUpdated, snapshot) = ast match {

      case obj: JsObj =>
        val contentUpdated = obj(contentUpdatedField).asBool.value
        contentUpdated -> {
          snapshotJsonCodec match {
            case codec: SnapshotCodec[T] => codec decode obj
            case _ =>
              val removeLength = 1 + contentUpdatedField.length + 3 + (if (contentUpdated) 4 else 5) + 1
              val reassembled = "{" concat json.substring(removeLength, json.length)
              snapshotJsonCodec decode reassembled
          }
        }

      case arr: JsArr =>
        val contentUpdated = arr(0).asBool.value
        contentUpdated -> {
          val removeLength = 1 + (if (contentUpdated) 4 else 5) + 1
          val reassembled = "[" concat json.substring(removeLength, json.length)
          snapshotJsonCodec decode reassembled
        }

      case _ => sys.error(s"Cannot decode snapshot update: $json")
    }
    new SnapshotUpdate(snapshot, contentUpdated)
  }

}

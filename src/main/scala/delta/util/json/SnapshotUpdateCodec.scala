package delta.util.json

import scuff.Codec
import delta.Snapshot
import delta.util.SnapshotUpdate

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
  protected def snapshotField: String = "snapshot"
  private[this] val contentUpdatedValueIndex = /* start brace */ 1 + /* quote */ 1 + contentUpdatedField.length() + /* quote */ 1 + /* colon */ 1

  def encode(update: SnapshotUpdate[T]): String = {
    val snapshotJson: String = (snapshotJsonCodec.encode(update.snapshot): String).trim
    s"""{"$contentUpdatedField":${update.contentUpdated},"$snapshotField":$snapshotJson}"""
  }

  def decode(json: String): SnapshotUpdate[T] = {
    new SnapshotUpdate(snapshot(json), contentUpdated(json))
  }

  /** Extract contentUpdated. */
  def contentUpdated(json: String): Boolean = json.charAt(contentUpdatedValueIndex) == 't'

  /** Extract snapshot. */
  def snapshot(json: String): Snapshot[T] = {
    val offset =
      contentUpdatedValueIndex +
        (if (contentUpdated(json)) 4 else 5) +
        1 + 1 + snapshotField.length + 1 + 1
    snapshotJsonCodec decode json.substring(offset, json.length - 1)
  }

  /** Extract snapshot content. */
  def snapshotContent(json: String): T = snapshotJsonCodec match {
    case codec: SnapshotCodec[T] => codec.content(json)
    case _ => this.snapshot(json).content
  }

}

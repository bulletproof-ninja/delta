package delta

case class Snapshot[+Content](
    content: Content,
    revision: Int,
    tick: Long) {

  def map[That](f: Content => That): Snapshot[That] = new Snapshot(f(content), revision, tick)
}

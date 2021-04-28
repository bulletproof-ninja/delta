package blogging.tests

import blogging._
import blogging.read.BlogPostQueryModel.BlogPost

import scuff.Reduction
import delta.Snapshot
import scala.collection.concurrent.TrieMap

class BlogPostConsumer[R](makeResult: TrieMap[BlogPostID, Snapshot[BlogPost]] => R)
extends Reduction[Snapshot[BlogPost], R] {
  private[this] val map = new TrieMap[BlogPostID, Snapshot[BlogPost]]
  def next(t: Snapshot[BlogPost]): Unit =
    map.update(t.state.id, t)
  def result(): R =
    makeResult(map)
}

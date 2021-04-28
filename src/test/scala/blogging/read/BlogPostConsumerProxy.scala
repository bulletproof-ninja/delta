package blogging.read

import blogging.BlogPostID
import blogging.read.BlogPostsProcessor.StoredState

import scuff.Reduction
import java.util.UUID
import delta.Snapshot
import blogging.read.BlogPostQueryModel.BlogPost

class BlogPostConsumerProxy[R](
  consumer: Reduction[Snapshot[BlogPost], R])
extends Reduction[(UUID, Snapshot[StoredState]), R] {
  def next(t: (UUID, Snapshot[StoredState])): Unit = {
    t match {
      case (uuid, Snapshot(bp: StoredState.BlogPost, revision, tick)) =>
        (bp toModel BlogPostID(uuid)) foreach {
          consumer next Snapshot(_, revision, tick)
        }
      case author =>
        sys.error(s"Should not have returned an author entry: $author")
    }

  }
  def result(): R = consumer.result()
}

package blogging.tests

import delta.testing.InMemoryProcStore
import java.{util => ju}
import blogging.read.BlogPostsProcessor.StoredState
import scala.concurrent.ExecutionContext

class BlogPostProcessStore(implicit ec: ExecutionContext)
extends InMemoryProcStore[ju.UUID, StoredState, StoredState](
  "blog-post-query-model")

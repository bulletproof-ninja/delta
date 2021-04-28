package blogging.write.blogpost

import blogging.BloggingEvent

import delta.EventStore

import java.util.UUID
import scala.concurrent.ExecutionContext

class Repository(
  es: EventStore[UUID, BloggingEvent])(
  implicit ec: ExecutionContext)
extends delta.write.EntityRepository(BlogPost)(es, ec)

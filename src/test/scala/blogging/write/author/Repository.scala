package blogging.write.author

import blogging.BloggingEvent

import delta.EventStore

import java.util.UUID
import scala.concurrent.ExecutionContext

class Repository(
  es: EventStore[UUID, BloggingEvent])(
  implicit ec: ExecutionContext)
extends delta.write.EntityRepository(Author)(es, ec)

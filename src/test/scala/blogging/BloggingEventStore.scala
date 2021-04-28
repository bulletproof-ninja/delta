package blogging

import delta._
import java.util.UUID

trait BloggingEventSource
extends EventSource[UUID, BloggingEvent]

trait BloggingEventStore
extends EventStore[UUID, BloggingEvent]
with BloggingEventSource

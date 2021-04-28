package blogging.write.blogpost.events

import java.time.LocalDateTime

import scala.{ SerialVersionUID => version }

import blogging._
import java.net.URI

sealed abstract class BlogPostEvent
extends BloggingEvent {
  type Callback = BlogPostEvents
}

trait BlogPostEvents {
  type Return

  def on(evt: BlogPostDrafted): Return
  def on(evt: BlogPostPublished): Return
  def on(evt: TagsUpdated): Return
}

@version(1)
case class BlogPostDrafted(
  author: AuthorID,
  headline: String,
  body: String,
  tags: Set[String])
extends BlogPostEvent { def dispatch(callback: BlogPostEvents) = callback on this }

@version(1)
case class BlogPostPublished(
  dateTime: LocalDateTime,
  externalLinks: Set[URI])
extends BlogPostEvent { def dispatch(callback: BlogPostEvents) = callback on this }

@version(1)
case class TagsUpdated(
  tags: Set[String])
extends BlogPostEvent { def dispatch(callback: BlogPostEvents) = callback on this }

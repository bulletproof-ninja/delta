package blogging.write.blogpost.state

import blogging.write.blogpost.events._

import java.time.LocalDateTime

private[blogpost] case class BlogPost(
  tags: Set[String],
  publishDate: Option[LocalDateTime] = None)

object Projector
extends delta.Projector[BlogPost, BlogPostEvent] {

  def init(evt: BlogPostEvent): BlogPost =
    evt dispatch new EventProjector

  def next(state: BlogPost, evt: BlogPostEvent): BlogPost =
    evt dispatch new EventProjector(state)

}

class EventProjector(blogPost: BlogPost = null)
extends BlogPostEvents {

  type Return = BlogPost

  def on(evt: BlogPostDrafted): BlogPost = {
    require(blogPost == null)
    new BlogPost(tags = evt.tags)
  }

  def on(evt: BlogPostPublished): BlogPost =
    blogPost.copy(publishDate = Some(evt.dateTime))

  def on(evt: TagsUpdated): BlogPost =
    blogPost.copy(tags = evt.tags)

}

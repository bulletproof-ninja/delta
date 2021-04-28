package blogging.write.blogpost

import blogging._
import java.time.LocalDateTime
import java.net.URI

case class DraftBlogPost(
  author: AuthorID,
  headline: String,
  body: String,
  tags: Set[String])

case class PublishBlogPost(
  dateTime: LocalDateTime,
  externalSites: Set[URI])

case class UpdateTags(tags: Set[String])

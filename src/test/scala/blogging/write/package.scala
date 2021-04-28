package blogging

import delta.write.Repository
import delta.write.MutableEntity

import blogging.write.author.Author
import blogging.write.blogpost.BlogPost
import scala.concurrent.Future

package object write {
  val channels = Set(Author, BlogPost).map(_.channel)
  implicit def newMetadata = new write.Metadata()
  // implicit def newMetadata(u: Unit): Future[delta.Metadata] = Future successful metadata
  type AuthorRepository = Repository[AuthorID, Author] with MutableEntity
  type BlogPostRepository = Repository[BlogPostID, BlogPost] with MutableEntity
}

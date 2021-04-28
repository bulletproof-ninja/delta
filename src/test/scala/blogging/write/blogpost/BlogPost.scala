package blogging.write.blogpost

import events._

import blogging.BlogPostID

import delta.write.Entity

object BlogPost
extends Entity("blog-post", state.Projector) {
  type Type = BlogPost
  type Id = BlogPostID

  protected def init(id: Id, stateRef: StateRef, concurrentUpdates: List[Transaction]) =
    new BlogPost(stateRef)
  protected def StateRef(blogPost: BlogPost) =
    blogPost.stateRef
  protected def validate(s: state.BlogPost) = ()

  def apply(cmd: DraftBlogPost): BlogPost = {
    val post = new BlogPost
    post.stateRef apply BlogPostDrafted(
      author = cmd.author,
      headline = cmd.headline,
      body = cmd.body,
      tags = cmd.tags)
    post
  }

}

final class BlogPost(
  private val stateRef: BlogPost.StateRef = BlogPost.newStateRef()) {
  private def blogPost = stateRef.get

  def apply(cmd: PublishBlogPost): Unit =
    blogPost.publishDate match {
      case None =>
        stateRef apply BlogPostPublished(cmd.dateTime, cmd.externalSites)
        assert(blogPost.publishDate contains cmd.dateTime)
      case Some(cmd.dateTime) => // Idempotent, do nothing
      case Some(earlier) =>
        throw new IllegalArgumentException(s"Has already been published on $earlier")
    }

  def apply(cmd: UpdateTags): Unit =
    if (blogPost.tags != cmd.tags) {
      stateRef apply TagsUpdated(cmd.tags)
      assert(blogPost.tags == cmd.tags)
    }

}

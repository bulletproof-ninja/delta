package blogging.tests

import blogging._
import blogging.write.blogpost.events.{ BlogPostEvent, BlogPostEvents }
import blogging.write.author.events.{ AuthorEvent, AuthorEvents }

import scuff.serialVersionUID
import scuff.json._

import delta.util.ReflectiveDecoder
import scuff.EmailAddress

import java.{util => ju}
import java.time.LocalDateTime
import java.net.URI

object JsonEventFormat
extends JsonEventFormat

class JsonEventFormat
extends ReflectiveDecoder[BloggingEvent, JSON]
with delta.EventFormat[BloggingEvent, JSON]
with JsonBlogPostEvents
with JsonAuthorEvents {

  type Return = JSON

  protected def getName(cls: Class[_ <: BloggingEvent]): String = {
    // Expected naming scheme: *.<entity>.events.<Event>
    val parts = cls.getName.split("\\.")
    val eventPart = parts(parts.length-1)
    val entityPart = parts(parts.length-3)
    s"$entityPart:$eventPart"
  }

  protected def getVersion(cls: Class[_ <: BloggingEvent]): Byte =
    serialVersionUID(cls).toByte

  def encode(evt: BloggingEvent): JSON = evt match {
    case evt: BlogPostEvent => evt dispatch this
    case evt: AuthorEvent => evt dispatch this
    case _ => sys.error(s"Unhandled event: $evt")
  }

}

trait JsonBlogPostEvents
extends BlogPostEvents {
  json: JsonEventFormat =>

  import blogging.write.blogpost.events._

  def on(evt: BlogPostDrafted): JSON = JsVal(evt).toJson.ensuring(_ contains """"tags":[""")
  def blogPostDrafted(enc: Encoded): BlogPostDrafted =
    enc.version {
      case 1 =>
        val obj = JsVal.parse(enc.data).asObj
        val authorId = AuthorID(ju.UUID fromString obj.author.asStr)
        BlogPostDrafted(
          authorId,
          obj.headline.asStr ,
          obj.body.asStr,
          obj.tags.asArr.map(_.asStr.value).toSet)
    }
  def on(evt: BlogPostPublished): JSON =
    JsObj(
      "pubDate" -> evt.dateTime.toString,
      "externalLinks" -> evt.externalLinks.map(_.toString).map(JsStr(_))
    ).toJson
  def blogPostPublished(enc: Encoded): BlogPostPublished =
    enc.version {
      case 1 => try {
        val obj = JsVal.parse(enc.data).asObj
        BlogPostPublished(
          LocalDateTime parse obj.pubDate.asStr,
          obj.externalLinks.asArr.map(_.asStr.value).map(new URI(_)).toSet)
      } catch {
        case e: RuntimeException =>
          throw new RuntimeException(s"Unexpected structure: ${enc.data}", e)
      }
    }
  def on(evt: TagsUpdated): JSON = JsArr(evt.tags.map(JsStr(_)).toSeq: _*).toJson
  def tagsUpdated(enc: Encoded): TagsUpdated =
    enc.version {
      case 1 =>
        TagsUpdated(JsVal.parse(enc.data).asArr.values.map(_.asStr.value).toSet)
    }
}

trait JsonAuthorEvents
extends AuthorEvents {
  json: JsonEventFormat =>

  import blogging.write.author.events._

  def on(evt: AuthorRegistered): JSON = JsObj("name" -> evt.name, "emailAddress" -> evt.emailAddress.toString).toJson
  def authorRegistered(enc: Encoded): AuthorRegistered =
    enc.version {
      case 1 =>
        val obj = JsVal.parse(enc.data).asObj
        AuthorRegistered(obj.name.asStr, EmailAddress(obj.emailAddress.asStr))
    }
  def on(evt: EmailAddressAdded): JSON = JsStr(evt.emailAddress.toString).toJson
  def emailAddressAdded(enc: Encoded): EmailAddressAdded =
    enc.version {
      case 1 =>
        EmailAddressAdded(EmailAddress(JsVal.parse(enc.data).asStr))
    }
  def on(evt: EmailAddressRemoved): JSON = JsStr(evt.emailAddress.toString).toJson
  def emailAddressRemoved(enc: Encoded): EmailAddressRemoved =
    enc.version {
      case 1 =>
        EmailAddressRemoved(EmailAddress(JsVal.parse(enc.data).asStr))
    }
  def on(evt: AuthorNameChanged): JSON = JsStr(evt.newName).toJson
  def authorNameChanged(enc: Encoded): AuthorNameChanged =
    enc.version {
      case 1 =>
        AuthorNameChanged(JsVal.parse(enc.data).asStr)
    }

}

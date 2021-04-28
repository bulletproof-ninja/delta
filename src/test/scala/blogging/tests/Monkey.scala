package blogging.tests

import scala.util.Random

object Monkey {
  def nextLetter(): Char = Random.between('a', 'z').toChar
  def nextWord(capitalized: Boolean = false): String =
    (if (capitalized) s"${Character toUpperCase nextLetter()}" else "") concat {
      (1 to Random.between(2, 12)).map { _ => nextLetter() }.mkString
    }

  def generateHeadline(): String = {
    val wordCount = Random.between(3, 8)
    (1 to wordCount).foldLeft(new java.lang.StringBuilder().append(nextWord(true))) {
      case (sb, _) =>
        sb append ' ' append nextWord()
    }.toString
  }
  def generateBlogText(minWordCount: Int): String = {
    val wordCount = Random.between(minWordCount, (minWordCount * 1.2f).toInt)
    val text = (1 to wordCount).foldLeft(new java.lang.StringBuilder().append(nextWord(true))) {
      case (sb, _) =>
        val newSentence = sb.charAt(sb.length-1) == '.'
        val word = nextWord(newSentence)
        if (newSentence) sb append word
        else {
          if (Random.nextInt(7) % 7 == 0) sb append ',' append ' ' append word
          else if (Random.nextInt(17) % 17 == 0) sb append ' ' append word append '.'
          else sb append ' ' append word
        }
        sb
    }
    if (text.charAt(text.length-1) != '.') text append '.'
    text.toString
  }

}

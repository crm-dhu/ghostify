package com.goku.ghostify.nlp

import com.goku.ghostify.PortalUnaryTransformer
import com.goku.ghostify.data.NamedFeature
import com.goku.ghostify.util.ObjectMarshaller
import com.johnsnowlabs.nlp.annotators.common.{
  IndexedToken,
  Sentence,
  SentenceSplit,
  TokenizedSentence,
  TokenizedWithSentence
}
import com.johnsnowlabs.nlp.{Annotation, AnnotatorType}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._

import java.util.regex.Pattern
import scala.util.matching.Regex

case class TokenizerModel(
  inputCol: NamedFeature[Array[Annotation]],
  outputCol: NamedFeature[Array[Annotation]],
  targetPattern: String = "\\S+",
  caseSensitive: Boolean = true,
  contextChars: Array[String] = Array(".", ",", ";", ":", "!", "?", "*", "-", "(", ")", "\"", "'")
) extends PortalUnaryTransformer[Array[Annotation], Array[Annotation]] {

  override def transformFeature(annotations: Array[Annotation]): Array[Annotation] = {
    val sentences = SentenceSplit.unpack(annotations)
    val tokenized = tag(sentences)
    TokenizedWithSentence.pack(tokenized).toArray
  }

  private val ProtectChar = "ↈ"
  private val BreakChar = "ↇ"
  private lazy val BreakPattern: Regex = ("[^(?:" + targetPattern + ")" + ProtectChar + "]").r
  private lazy val SplitPattern: Regex = ("[^" + BreakChar + "]+").r

  def buildRuleFactory: Array[String] = {

    val quotedContext = Pattern.quote(contextChars.mkString(""))
    val processedPrefix = s"\\A([$quotedContext]*)"
    val processedSuffix = s"([$quotedContext]*)\\z"
    val processedInfixes = Array(s"([^$quotedContext](?:.*[^$quotedContext])*)")
    processedPrefix +: processedInfixes :+ processedSuffix
  }

  lazy val rules = buildRuleFactory

  def tag(sentences: Seq[Sentence]): Seq[TokenizedSentence] = {
    sentences.map { text =>
      val protectedText = BreakPattern.replaceAllIn(text.content, BreakChar)
      val regRules = rules.map(_.r)
      val matchedTokens = SplitPattern.findAllMatchIn(protectedText).toSeq

      val tokens = matchedTokens
        .flatMap { candidate =>
          val matched =
            regRules.flatMap(r => r.findFirstMatchIn(candidate.matched)).filter(_.matched.nonEmpty)
          if (matched.nonEmpty) {
            matched.flatMap { m =>
              var curPos = m.start
              (1 to m.groupCount)
                .flatMap { i =>
                  val target = m.group(i)

                  def defaultCandidate = {
                    val it = IndexedToken(
                      target,
                      text.start + candidate.start + curPos,
                      text.start + candidate.start + curPos + target.length - 1
                    )
                    curPos += target.length
                    Seq(it)
                  }

                  defaultCandidate
                }
            }
          } else {
            Array(
              IndexedToken(
                candidate.matched,
                text.start + candidate.start,
                text.start + candidate.end - 1
              )
            )
          }
        }
        .filter(t => t.token.nonEmpty)
        .toArray
      TokenizedSentence(tokens, text.index)
    }
  }
  def marshal = this.asJson
}

object TokenizerModel extends ObjectMarshaller[TokenizerModel] {

  def unmarshal(jsonObj: Json): Either[Throwable, TokenizerModel] = jsonObj.as[TokenizerModel]
}

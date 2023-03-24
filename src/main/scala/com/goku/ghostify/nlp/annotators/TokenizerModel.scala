package com.goku.ghostify.nlp.annotators

import scala.util.matching.Regex

import com.goku.ghostify.nlp.{Annotation, AnnotatorModel, AnnotatorType, HasSimpleAnnotate}
import com.goku.ghostify.nlp.annotators.common.{IndexedToken, Sentence, SentenceSplit, TokenizedSentence}
import com.johnsnowlabs.nlp.AnnotatorType.DOCUMENT
import org.apache.spark.ml.param.{BooleanParam, Param, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import AnnotatorType.TOKEN

import com.salesforce.mce.ghostify.nlp._

class TokenizerModel(override val uid: String)
    extends AnnotatorModel[TokenizerModel] with HasSimpleAnnotate[TokenizerModel] {

  val rules: StringArrayParam = new StringArrayParam(this, "rules", "regex rules")

  val caseSensitive: BooleanParam =
    new BooleanParam(this, "caseSensitive", "Whether to care for case sensitiveness")

  val targetPattern: Param[String] =
    new Param(this, "targetPattern", "pattern to grab from text as token candidates. Defaults \\S+")

  setDefault(targetPattern -> "\\S+", caseSensitive -> true)

  override val outputAnnotatorType: AnnotatorType = TOKEN

  override val inputAnnotatorTypes: Array[AnnotatorType] = Array[AnnotatorType](DOCUMENT)

  def this() = this(Identifiable.randomUID("REGEX_TOKENIZER"))

  def setRules(values: Array[String]): this.type = set(rules, values)

  def getRules: Array[String] = $(rules)

  def setTargetPattern(value: String): this.type = set(targetPattern, value)

  def getTargetPattern: String = $(targetPattern)

  def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)

  def getCaseSensitive(value: Boolean): Boolean = $(caseSensitive)

  private val ProtectChar = "ↈ"
  private val BreakChar = "ↇ"

  private lazy val BreakPattern: Regex =
    ("[^(?:" + $(targetPattern) + ")" + ProtectChar + "]").r
  private lazy val SplitPattern: Regex =
    ("[^" + BreakChar + "]+").r

  def tag(sentences: Seq[Sentence]): Seq[TokenizedSentence] = {
    sentences.map { text =>
      val protectedText = BreakPattern.replaceAllIn(text.content, BreakChar)
      val regRules = $(rules).map(_.r)
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

  override def annotate(annotations: Seq[Annotation]): Seq[Annotation] = {
    val sentences = SentenceSplit.unpack(annotations)
    val tokenized = tag(sentences)

    tokenized.flatMap { sentence =>
      sentence.indexedTokens.map { token =>
        Annotation(
          AnnotatorType.TOKEN,
          token.begin,
          token.end,
          token.token,
          Map("sentence" -> sentence.sentenceIndex.toString)
        )
      }
    }
  }
}

object TokenizerModel extends DefaultParamsReadable[TokenizerModel]

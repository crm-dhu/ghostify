package com.vegeta.goku.ghostify.nlp

import java.util.regex.Pattern

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param.{BooleanParam, Param, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Dataset

import com.vegeta.goku.ghostify.nlp.AnnotatorType.{DOCUMENT, TOKEN}

class Tokenizer(override val uid: String) extends AnnotatorApproach[TokenizerModel] {

  override val outputAnnotatorType: AnnotatorType = TOKEN

  override val inputAnnotatorTypes: Array[AnnotatorType] = Array[AnnotatorType](DOCUMENT)

  def this() = this(Identifiable.randomUID("REGEX_TOKENIZER"))

  override val description: String =
    "Annotator that identifies points of analysis in a useful manner"

  val caseSensitive: BooleanParam =
    new BooleanParam(this, "caseSensitiveExceptions", "Whether to care for case sensitiveness")

  val targetPattern: Param[String] =
    new Param(this, "targetPattern", "Pattern to grab from text as token candidates. Defaults \\S+")

  val contextChars: StringArrayParam = new StringArrayParam(
    this,
    "contextChars",
    "Character list used to separate from token boundaries"
  )

  def setTargetPattern(value: String): this.type = set(targetPattern, value)

  def getTargetPattern: String = $(targetPattern)

  def setContextChars(v: Array[String]): this.type = {
    require(v.forall(_.length == 1), "All elements in context chars must have length == 1")
    set(contextChars, v)
  }

  def addContextChars(v: String): this.type = {
    require(v.length == 1, "Context char must have length == 1")
    set(contextChars, get(contextChars).getOrElse(Array.empty[String]) :+ v)
  }

  def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)

  def getCaseSensitive(value: Boolean): Boolean = $(caseSensitive)

  setDefault(
    inputCols -> Array(DOCUMENT),
    outputCol -> "token",
    targetPattern -> "\\S+",
    contextChars -> Array(".", ",", ";", ":", "!", "?", "*", "-", "(", ")", "\"", "'"),
    caseSensitive -> true
  )

  def buildRuleFactory: Array[String] = {

    val quotedContext = Pattern.quote($(contextChars).mkString(""))

    val processedPrefix = s"\\A([$quotedContext]*)"

    val processedSuffix = s"([$quotedContext]*)\\z"

    val processedInfixes = Array(s"([^$quotedContext](?:.*[^$quotedContext])*)")

    processedPrefix +: processedInfixes :+ processedSuffix
  }

  override def train(
    dataset: Dataset[_],
    recursivePipeline: Option[PipelineModel]
  ): TokenizerModel = {

    val rules = buildRuleFactory

    val raw = new TokenizerModel()
      .setRules(rules)
      .setCaseSensitive($(caseSensitive))
      .setTargetPattern($(targetPattern))

    raw

  }

}

object Tokenizer extends DefaultParamsReadable[Tokenizer]

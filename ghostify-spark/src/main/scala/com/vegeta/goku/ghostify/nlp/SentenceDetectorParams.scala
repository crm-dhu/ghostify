package com.salesforce.mce.ghostify.nlp

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.param.{BooleanParam, IntParam, Params}

trait SentenceDetectorParams extends Params {

  val explodeSentences = new BooleanParam(
    this,
    "explodeSentences",
    "whether to explode each sentence into a different row, for better parallelization. Defaults to false."
  )

  val splitLength: IntParam =
    new IntParam(this, "splitLength", "length at which sentences will be forcibly split.")

  val minLength =
    new IntParam(this, "minLength", "Set the minimum allowed length for each sentence")

  val maxLength =
    new IntParam(this, "maxLength", "Set the maximum allowed length for each sentence")

  setDefault(
    explodeSentences -> false,
    minLength -> 0
  )

  def setExplodeSentences(value: Boolean): this.type = set(explodeSentences, value)

  def getExplodeSentences: Boolean = $(explodeSentences)

  def setSplitLength(value: Int): this.type = set(splitLength, value)

  def getSplitLength: Int = $(splitLength)

  def setMinLength(value: Int): this.type = {
    require(value >= 0, "minLength must be greater equal than 0")
    require(value.isValidInt, "minLength must be Int")
    set(minLength, value)
  }

  def getMinLength(value: Int): Int = $(minLength)

  def setMaxLength(value: Int): this.type = {
    require(
      value >= $ {
        minLength
      },
      "maxLength must be greater equal than minLength"
    )
    require(value.isValidInt, "minLength must be Int")
    set(maxLength, value)
  }

  def getMaxLength(value: Int): Int = $(maxLength)

  def truncateSentence(sentence: String, maxLength: Int): Array[String] = {
    var currentLength = 0
    val allSentences = ArrayBuffer.empty[String]
    val currentSentence = ArrayBuffer.empty[String]

    def addWordToSentence(word: String): Unit = {

      currentLength += word.length + 1
      currentSentence.append(word)
    }

    sentence
      .split(" ")
      .foreach(word => {
        if (currentLength + word.length > maxLength) {
          allSentences.append(currentSentence.mkString(" "))
          currentSentence.clear()
          currentLength = 0
          addWordToSentence(word)
        } else {
          addWordToSentence(word)
        }
      })

    allSentences.append(currentSentence.mkString(" "))
    allSentences.toArray
  }

}

package com.salesforce.mce.ghostify.nlp

class SimpleSentenceExtractor(sourceText: String) {

  final val SentenceSplitter = "(?<=[^A-Z].[.?]) +(?=[A-Z])".r

  private def buildSentenceProperties(
    rawSentences: Array[String],
    sourceText: String
  ): Array[Sentence] = {
    var lastCharPosition = 0
    rawSentences.filter(_.nonEmpty).zipWithIndex.map { case (rawSentence, index) =>
      val trimmed = rawSentence.trim
      val startPad = sourceText.indexOf(trimmed, lastCharPosition)

      val sentence = Sentence(trimmed, startPad, startPad + trimmed.length() - 1, index)
      lastCharPosition = sentence.end + 1

      sentence
    }
  }

  def pull: Array[Sentence] = {

    val splitSentences = SentenceSplitter.split(sourceText)
    buildSentenceProperties(splitSentences, sourceText)

  }

}

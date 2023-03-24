package com.goku.ghostify.nlp.annotators.ner

import scala.collection.mutable.ArrayBuffer

import com.goku.ghostify.nlp.Annotation
import com.goku.ghostify.nlp.annotators.common.Annotated.NerTaggedSentence

object NerTagsEncoding {

  def fromIOB(
    sentence: NerTaggedSentence,
    doc: Annotation,
    sentenceIndex: Int = 0,
    originalOffset: Boolean = true,
    includeNoneEntities: Boolean = false,
    format: String = "IOB2"
  ): Seq[NamedEntity] = {

    val noChunk = "O"
    var beginningTagChunk = "B-"
    if (format != "IOB2") {
      beginningTagChunk = "I-"
    }

    val result = ArrayBuffer[NamedEntity]()

    val words = sentence.words.length

    var lastTag: Option[String] = None
    var lastTagStart = -1

    def flushEntity(startIdx: Int, endIdx: Int): Unit = {
      val start = sentence.indexedTaggedWords(startIdx).begin - doc.begin
      val end = sentence.indexedTaggedWords(endIdx).end - doc.begin
      require(
        start <= end && end <= doc.result.length,
        s"Failed to flush entities in NerConverter. " +
          s"Chunk offsets $start - $end are not within tokens:\n${sentence.words
            .mkString("||")}\nfor sentence:\n${doc.result}"
      )
      val confidenceArray =
        sentence.indexedTaggedWords.slice(startIdx, endIdx + 1).flatMap(_.metadata.values)
      val finalConfidenceArray =
        try {
          confidenceArray.map(x => x.trim.toFloat)
        } catch {
          case _: Exception => Array.empty[Float]
        }
      val confidence =
        if (finalConfidenceArray.isEmpty) None
        else Some(finalConfidenceArray.sum / finalConfidenceArray.length)
      val content =
        if (originalOffset) doc.result.substring(start, end + 1)
        else sentence.indexedTaggedWords(startIdx).word
      val entity = NamedEntity(
        sentence.indexedTaggedWords(startIdx).begin,
        sentence.indexedTaggedWords(endIdx).end,
        lastTag.get,
        content,
        sentenceIndex.toString,
        confidence
      )
      result.append(entity)
      lastTag = None

    }

    def getTag(tag: String): Option[String] = {
      try {
        lastTag = Some(tag.substring(2))
      } catch {
        case e: StringIndexOutOfBoundsException =>
          require(
            tag.length < 2,
            s"This annotator only supports IOB and IOB2 tagging: https://en.wikipedia.org/wiki/Inside%E2%80%93outside%E2%80%93beginning_(tagging) \n $e"
          )
      }
      lastTag
    }

    for (i <- 0 until words) {
      val tag = sentence.tags(i)
      if (lastTag.isDefined && (tag.startsWith(beginningTagChunk) || tag == noChunk)) {
        flushEntity(lastTagStart, i - 1)
      }

      if (includeNoneEntities && lastTag.isEmpty) {
        lastTag = if (tag == noChunk) Some(tag) else getTag(tag)
        lastTagStart = i
      } else {
        if (lastTag.isEmpty && tag != noChunk) {
          lastTag = getTag(tag)
          lastTagStart = i
        }
      }
    }

    if (lastTag.isDefined) {
      flushEntity(lastTagStart, words - 1)
    }
    result.toList
  }

}

case class NamedEntity(
  start: Int,
  end: Int,
  entity: String,
  text: String,
  sentenceId: String,
  confidence: Option[Float]
)

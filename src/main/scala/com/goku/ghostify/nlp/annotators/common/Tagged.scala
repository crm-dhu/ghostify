package com.goku.ghostify.nlp.annotators.common

import scala.collection.Map

import com.goku.ghostify.nlp.Annotation
import com.goku.ghostify.nlp.AnnotatorType.{NAMED_ENTITY, POS}
import Annotated.{NerTaggedSentence, PosTaggedSentence}

trait Tagged[T >: TaggedSentence <: TaggedSentence] extends Annotated[T] {
  val emptyTag = "O"

  override def unpack(annotations: Seq[Annotation]): Seq[T] = {

    val tokenized = TokenizedWithSentence.unpack(annotations)

    val tagAnnotations = annotations
      .filter(a => a.annotatorType == annotatorType)
      .sortBy(a => a.begin)
      .toIterator

    var annotation: Option[Annotation] = None

    tokenized.map { sentence =>
      val tokens = sentence.indexedTokens.map { token =>
        while (tagAnnotations.hasNext && (annotation.isEmpty || annotation.get.begin < token.begin))
          annotation = Some(tagAnnotations.next)

        val tag = if (annotation.isDefined && annotation.get.begin == token.begin) {
          annotation.get.result
        } else
          emptyTag
        // etract the confidence score belong to the tag
        val metadata =
          try {
            if (annotation.get.metadata.isDefinedAt("confidence"))
              Map(tag -> annotation.get.metadata("confidence"))
            else
              Map(tag -> annotation.get.metadata(tag))
          } catch {
            case _: Exception =>
              Map.empty[String, String]
          }

        IndexedTaggedWord(token.token, tag, token.begin, token.end, metadata = metadata)
      }

      new TaggedSentence(tokens)
    }
  }

  override def pack(items: Seq[T]): Seq[Annotation] = {
    items.zipWithIndex.flatMap { case (item, sentenceIndex) =>
      item.indexedTaggedWords.map { tag =>
        val metadata: Map[String, String] = if (tag.confidence.isDefined) {
          Map("word" -> tag.word) ++ tag.confidence
            .getOrElse(Array.empty[Map[String, String]])
            .flatten ++
            Map("sentence" -> sentenceIndex.toString)
        } else {
          Map("word" -> tag.word) ++ Map.empty[String, String] ++ Map(
            "sentence" -> sentenceIndex.toString
          )
        }
        new Annotation(annotatorType, tag.begin, tag.end, tag.tag, metadata)
      }
    }
  }
}

object PosTagged extends Tagged[PosTaggedSentence] {
  override def annotatorType: String = POS
}

object NerTagged extends Tagged[NerTaggedSentence] {
  override def annotatorType: String = NAMED_ENTITY
}

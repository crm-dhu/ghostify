package com.goku.ghostify.nlp.annotators.common

import com.goku.ghostify.nlp.{Annotation, AnnotatorType}

object TokenizedWithSentence extends Annotated[TokenizedSentence] {

  override def annotatorType: String = AnnotatorType.TOKEN

  override def unpack(annotations: Seq[Annotation]): Seq[TokenizedSentence] = {
    val tokens = annotations
      .filter(_.annotatorType == annotatorType)
      .toArray

    val sentences = SentenceSplit.unpack(annotations)

    sentences
      .map(sentence => {
        val sentenceTokens = tokens
          .filter(token => token.begin >= sentence.start & token.end <= sentence.end)
          .map(token => IndexedToken(token.result, token.begin, token.end))
        sentenceTokens
      })
      .zipWithIndex
      .map { case (indexedTokens, index) => TokenizedSentence(indexedTokens, index) }
      .filter(_.indexedTokens.nonEmpty)
  }

  override def pack(sentences: Seq[TokenizedSentence]): Seq[Annotation] = {
    sentences.flatMap { sentence =>
      sentence.indexedTokens.map { token =>
        Annotation(
          annotatorType,
          token.begin,
          token.end,
          token.token,
          Map("sentence" -> sentence.sentenceIndex.toString)
        )
      }
    }
  }
}

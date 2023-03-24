package com.goku.ghostify.nlp.annotators.common

import com.goku.ghostify.nlp.Annotation

trait Annotated[T] {
  def annotatorType: String

  def unpack(annotations: Seq[Annotation]): Seq[T]

  def pack(items: Seq[T]): Seq[Annotation]
}

object Annotated {
  type PosTaggedSentence = TaggedSentence
  type NerTaggedSentence = TaggedSentence
}

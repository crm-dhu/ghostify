package com.goku.ghostify.nlp.annotators.common

case class TaggedSentence(
  taggedWords: Array[TaggedWord],
  indexedTaggedWords: Array[IndexedTaggedWord] = Array()
) {
  def this(indexedTaggedWords: Array[IndexedTaggedWord]) =
    this(indexedTaggedWords.map(_.toTaggedWord), indexedTaggedWords)

  val words: Array[String] = taggedWords.map(_.word)

  val tags: Array[String] = taggedWords.map(_.tag)

  def tupleWords: Array[(String, String)] = words.zip(tags)

  def mapWords: Map[String, String] = tupleWords.toMap
}

object TaggedSentence {
  def apply(indexedTaggedWords: Array[IndexedTaggedWord]) =
    new TaggedSentence(indexedTaggedWords.map(_.toTaggedWord), indexedTaggedWords)
}

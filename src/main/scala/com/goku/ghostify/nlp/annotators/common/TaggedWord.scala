package com.goku.ghostify.nlp.annotators.common

import scala.collection.Map

case class TaggedWord(word: String, tag: String)

case class IndexedTaggedWord(
  word: String,
  tag: String,
  begin: Int = 0,
  end: Int = 0,
  confidence: Option[Array[Map[String, String]]] = None,
  metadata: Map[String, String] = Map()
) {
  def toTaggedWord: TaggedWord = TaggedWord(this.word, this.tag)
}

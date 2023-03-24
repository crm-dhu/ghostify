package com.goku.ghostify.nlp.annotators.common

case class TokenizedSentence(indexedTokens: Array[IndexedToken], sentenceIndex: Int) {
  lazy val tokens: Array[String] = indexedTokens.map(t => t.token)

  def condense: String = tokens.mkString(" ")
}

package com.goku.ghostify.common

case class TokenizedSentence(indexedTokens: Array[IndexedToken], sentenceIndex: Int) {
  lazy val tokens: Array[String] = indexedTokens.map(t => t.token)
}

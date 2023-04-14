package com.goku.ghostify.nlp

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import com.goku.ghostify.common.{IndexedToken, TokenPiece}

// Refer to the implementation in `com.johnsnowlabs.nlp.annotators.tokenizer.wordpiece.WordpieceEncoder`
class WordpieceEncoder(
  vocabulary: Map[String, Int],
  unkToken: String = "[UNK]",
  maxInputCharsPerWord: Int = 200,
  partPrefix: String = "##"
) extends Serializable {

  require(vocabulary.contains(unkToken), "token " + unkToken + " not found in vocabulary")

  def encode(token: IndexedToken): Array[TokenPiece] = {
    val unkId = vocabulary(unkToken)

    if (token.token.length > maxInputCharsPerWord)
      Array(
        TokenPiece(unkToken, token.token, unkId, isWordStart = true, token.begin, token.end)
      )
    else {
      val result = ArrayBuffer[TokenPiece]()
      findTokenPiece(0, token.token.length, token, unkId, result)
      result.toArray
    }

  }

  @tailrec
  private def findTokenPiece(
    start: Int,
    end: Int,
    token: IndexedToken,
    unkId: Int,
    result: ArrayBuffer[TokenPiece]
  ): Array[TokenPiece] = {
    if (end <= start || start >= token.token.length) result.toArray
    else {
      val toFind = (if (start > 0) partPrefix else "") + token.token.substring(start, end)
      val found = vocabulary.get(toFind)
      if (found.nonEmpty) {
        val subToken = TokenPiece(
          toFind,
          token.token,
          found.get,
          start == 0,
          token.begin + start,
          token.begin + end - 1
        )
        result.append(subToken)
        findTokenPiece(end, token.token.length, token, unkId, result)
      } else {
        if (end - 1 == start)
          result.append(
            TokenPiece(unkToken, token.token, unkId, isWordStart = true, token.begin, token.end)
          )
        findTokenPiece(start, end - 1, token, unkId, result)
      }
    }
  }
}

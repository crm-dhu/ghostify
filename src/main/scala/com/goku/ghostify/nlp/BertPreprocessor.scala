package com.goku.ghostify.nlp

import com.goku.ghostify.common.{BertEncodedInput, Sentence, TokenPiece, WordpieceTokenizedSentence}

class BertPreprocessor(
  vocabulary: Map[String, Int],
  maxSentenceLength: Int = 128,
  caseSensitive: Boolean = true
) extends Serializable {
  val basicTokenizer = new BasicTokenizer(caseSensitive)
  val wordpieceEncoder = new WordpieceEncoder(vocabulary)

  protected val PadTokenId = 0

  protected val PadTokenPiece = TokenPiece("", "", PadTokenId, true, -1, -1)

  protected val StartToken = "[CLS]"

  protected val StartTokenId: Int =
    if (vocabulary.contains(StartToken)) vocabulary(StartToken)
    else throw new Exception("Cannot load CLS token from vocabs!")

  protected val StartTokenPiece = TokenPiece(StartToken, StartToken, StartTokenId, true, -1, -1)

  protected val EndToken = "[SEP]"

  protected val EndTokenId: Int =
    if (vocabulary.contains(EndToken)) vocabulary(EndToken)
    else throw new Exception("Cannot load SEP token from vocabs!")

  protected val EndTokenPiece = TokenPiece(EndToken, EndToken, EndTokenId, true, -1, -1)

  def encode(sentences: Seq[Sentence]) = {
    val preTokenizedSentences =
      sentences.map(sentence => basicTokenizer.tokenize(sentence).filter(_.token.trim.nonEmpty))

    val wordPieceTokenizedSentences = preTokenizedSentences.map { tokens =>
      val wordpieceTokens =
        tokens.flatMap { token =>
          val encoded = wordpieceEncoder.encode(token)
          encoded.map(r => r.copy(begin = token.end - token.token.length + 1, end = token.end))
        }
      WordpieceTokenizedSentence(wordpieceTokens)
    }

    val maxSeqLength =
      Array(maxSentenceLength - 2, wordPieceTokenizedSentences.map(_.tokens.length).max).min
    wordPieceTokenizedSentences.map { r =>
      val tokenPieces = r.tokens.take(maxSeqLength)
      val padding = Array.fill(maxSentenceLength - maxSeqLength - 2)(PadTokenPiece)
      BertEncodedInput(
        Array(StartTokenPiece) ++ tokenPieces ++ Array(EndTokenPiece) ++ padding,
        tokenPieces.length
      )
    }
  }
}

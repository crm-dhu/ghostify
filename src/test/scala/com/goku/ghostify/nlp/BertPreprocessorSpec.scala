package com.goku.ghostify.nlp

import com.goku.ghostify.common.{BertEncodedInput, Sentence, TokenPiece}
import org.scalatest.wordspec.AnyWordSpec

class BertPreprocessorSpec extends AnyWordSpec {

  val vocabs =
    Map(
      "[UNK]" -> 1,
      "[CLS]" -> 2,
      "[SEP]" -> 3,
      "happy" -> 4,
      "birth" -> 5,
      "##day" -> 6,
      "new" -> 7,
      "!" -> 8
    )
  val bertProcessor = new BertPreprocessor(vocabs, 10, false)

  "Bert preprocessor" should {

    "encode short text correctly" in {

      val text = "Happy birthday, Alex!"
      val encoded = bertProcessor.encode(Seq(Sentence(text, 0, text.length - 1, 0)))
      val expected = Seq(
        BertEncodedInput(
          Array(
            TokenPiece("[CLS]", "[CLS]", 2, true, -1, -1),
            TokenPiece("happy", "happy", 4, true, 0, 4),
            TokenPiece("birth", "birthday", 5, true, 6, 13),
            TokenPiece("##day", "birthday", 6, false, 6, 13),
            TokenPiece("[UNK]", ",", 1, true, 14, 14),
            TokenPiece("[UNK]", "alex", 1, true, 16, 19),
            TokenPiece("!", "!", 8, true, 20, 20),
            TokenPiece("[SEP]", "[SEP]", 3, true, -1, -1),
            TokenPiece("", "", 0, true, -1, -1),
            TokenPiece("", "", 0, true, -1, -1)
          ),
          6
        )
      )
      encoded.zip(expected).foreach { case (en, ex) =>
        assert(en.wordpieceTokens === ex.wordpieceTokens)
        assert(en.tokenLength === ex.tokenLength)
      }
    }

    "encode long text correctly" in {
      val text = "Happy new year and happy birthday, Alex!"
      val encoded = bertProcessor.encode(Seq(Sentence(text, 0, text.length - 1, 0)))
      val expected = Seq(
        BertEncodedInput(
          Array(
            TokenPiece("[CLS]", "[CLS]", 2, true, -1, -1),
            TokenPiece("happy", "happy", 4, true, 0, 4),
            TokenPiece("new", "new", 7, true, 6, 8),
            TokenPiece("[UNK]", "year", 1, true, 10, 13),
            TokenPiece("[UNK]", "and", 1, true, 15, 17),
            TokenPiece("happy", "happy", 4, true, 19, 23),
            TokenPiece("birth", "birthday", 5, true, 25, 32),
            TokenPiece("##day", "birthday", 6, false, 25, 32),
            TokenPiece("[UNK]", ",", 1, true, 33, 33),
            TokenPiece("[SEP]", "[SEP]", 3, true, -1, -1)
          ),
          8
        )
      )
      encoded.zip(expected).foreach { case (en, ex) =>
        assert(en.wordpieceTokens === ex.wordpieceTokens)
        assert(en.tokenLength === ex.tokenLength)
      }
    }

  }

}

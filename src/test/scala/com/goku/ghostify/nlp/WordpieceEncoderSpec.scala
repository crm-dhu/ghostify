package com.goku.ghostify.nlp

import com.goku.ghostify.common.{IndexedToken, TokenPiece}
import org.scalatest.wordspec.AnyWordSpec

class WordpieceEncoderSpec extends AnyWordSpec {

  "Wordpiece Encoder" should {
    val vocab = Map("[UNK]" -> 0, "a" -> 1, "##b" -> 2, "##c" -> 3)
    val wordpieceEncoder = new WordpieceEncoder(vocab)

    "encode existing word correctly" in {
      val encoded = wordpieceEncoder.encode(IndexedToken("abc", 0, 3))
      val expected = Array(
        TokenPiece("a", "abc", 1, true, 0, 0),
        TokenPiece("##b", "abc", 2, false, 1, 1),
        TokenPiece("##c", "abc", 3, false, 2, 2)
      )
      assert(encoded.length === expected.length)
      encoded.zip(expected).foreach { case (en, ex) => assert(en === ex) }
    }

    "encode non-existing word correctly" in {
      val encoded = wordpieceEncoder.encode(IndexedToken("bca", 3, 5))
      val expected = Array(TokenPiece("[UNK]", "bca", 0, true, 3, 5))
      assert(encoded.length === expected.length)
      encoded.zip(expected).foreach { case (en, ex) => assert(en === ex) }
    }
  }
}

package com.vegeta.goku.ghostify

import java.util

import ai.djl.modality.Classifications
import ai.djl.modality.nlp.Vocabulary
import ai.djl.modality.nlp.bert.BertFullTokenizer
import ai.djl.ndarray.NDList
import ai.djl.translate.{Batchifier, Translator, TranslatorContext}

class AnonymizerTranslator(tokenizer: BertFullTokenizer, length: Int)
  extends Translator[String, Classifications] {

  private val vocab: Vocabulary = tokenizer.getVocabulary
  private val ranks: util.List[String] = util.Arrays.asList("1", "2", "3", "4", "5")

  override def getBatchifier: Batchifier = Batchifier.STACK

  override def processInput(ctx: TranslatorContext, input: String): NDList = {
    val tokens = this.tokenizer.tokenize(input)
    val indices = new Array[Long](length)
    val mask = new Array[Long](length)
    val segmentIds = new Array[Long](length)
    val size = Math.min(length, tokens.size)
    for (i <- 0 until size) {
      indices(i + 1) = vocab.getIndex(tokens.get(i))
    }
    util.Arrays.fill(mask, 0, size, 1)
    val m = ctx.getNDManager
    new NDList(m.create(indices), m.create(mask), m.create(segmentIds))
  }

  override def processOutput(ctx: TranslatorContext, list: NDList) =
    new Classifications(ranks, list.singletonOrThrow.softmax(0))
}
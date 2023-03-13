package com.vegeta.goku.ghostify

import java.nio.file.Paths

import ai.djl.modality.Classifications
import ai.djl.modality.nlp.DefaultVocabulary
import ai.djl.modality.nlp.bert.BertFullTokenizer
import ai.djl.repository.zoo.Criteria
import ai.djl.training.util.ProgressBar

object Anonymizer {
  def main(args: Array[String]): Unit = {

    val modelDir = Paths.get("build/tensorflow/bert-rank")
    val vocabFile = modelDir.resolve("vocab.txt")
    val vocabulary = DefaultVocabulary.builder
      .optMinFrequency(1)
      .addFromTextFile(vocabFile)
      .optUnknownToken("[UNK]")
      .build
    val tokenizer = new BertFullTokenizer(vocabulary, true)
    val maxTokenLength = 128

    val translator = new AnonymizerTranslator(tokenizer, maxTokenLength)

    val criteria = Criteria.builder
      .setTypes(classOf[String], classOf[Classifications])
      .optModelPath(modelDir)
      .optTranslator(translator)
      .optProgress(new ProgressBar)
      .build

    val model = criteria.loadModel

    val review = "It works great, but it takes too long to update itself and slows the system"

    val predictor = model.newPredictor
    val classifications = predictor.predict(review)
    println(classifications)

  }

}
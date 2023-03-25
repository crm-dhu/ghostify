package com.goku.ghostify.nlp

import com.goku.ghostify.PortalBinaryTransformer
import com.goku.ghostify.data.NamedFeature
import com.goku.ghostify.util.ObjectMarshaller
import com.johnsnowlabs.ml.tensorflow.TensorflowWrapper
import com.johnsnowlabs.ml.util.LoadExternalModel.{loadTextAsset, modelSanityCheck, notSupportedEngineError}
import com.johnsnowlabs.ml.util.ModelEngine
import com.johnsnowlabs.nlp.Annotation
import com.johnsnowlabs.nlp.AnnotatorType.{DOCUMENT, TOKEN}
import com.johnsnowlabs.nlp.annotators.common.{IndexedToken, Sentence, TokenizedSentence, TokenizedWithSentence}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._

case class BertForTokenClassification(
  inputCols: (NamedFeature[Array[Annotation]], NamedFeature[Array[Annotation]]),
  outputCol: NamedFeature[Array[Annotation]],
  modelPath: String,
  batchSize: Int = 8,
  configProtoBytes: Array[Int] = Array.empty[Int],
  maxSentenceLength: Int = 128,
  caseSensitive: Boolean = true
) extends PortalBinaryTransformer[Array[Annotation], Array[Annotation], Array[Annotation]] {

  require(
    maxSentenceLength <= 512,
    "BERT models do not support sequences longer than 512 because of trainable positional embeddings."
  )

  val (localModelPath, detectedEngine) = modelSanityCheck(modelPath)
  val vocabs: Map[String, Int] = loadTextAsset(localModelPath, "vocab.txt").zipWithIndex.toMap
  val labels: Map[String, Int] = loadTextAsset(localModelPath, "labels.txt").zipWithIndex.toMap

  val model: BertClassification = {
    if (detectedEngine == ModelEngine.tensorflow) {
      val (wrapper, signatures) =
        TensorflowWrapper.read(localModelPath, zipped = false, useBundle = true)

      if (signatures.isEmpty) throw new Exception("Cannot load signature definitions from model!")

      val sentenceStartTokenId =
        if (vocabs.contains("[CLS]")) vocabs("[CLS]")
        else throw new Exception("Cannot load CLS token from vocabs!")
      val sentenceEndTokenId =
        if (vocabs.contains("[SEP]")) vocabs("[SEP]")
        else throw new Exception("Cannot load SEP token from vocabs!")

      new BertClassification(
        wrapper,
        sentenceStartTokenId,
        sentenceEndTokenId,
        configProtoBytes = Some(configProtoBytes.map(_.toByte)),
        labels,
        signatures,
        vocabs
      )

    } else throw new Exception(notSupportedEngineError)
  }

  override def transformFeature(
    annotations: (Array[Annotation], Array[Annotation])
  ): Array[Annotation] = {
    val (docs, tokens) = annotations
    val tokenizedSentences = TokenizedWithSentence.unpack(docs ++ tokens)
    val batchedTokenizedSentences = tokenizedSentences.grouped(batchSize)
    if (batchedTokenizedSentences.nonEmpty) {
      batchedTokenizedSentences
        .flatMap(batch => model.predict(batch, batchSize, maxSentenceLength, caseSensitive, labels))
        .toArray
    } else Array.empty[Annotation]
  }

  def marshal = this.asJson
}

object BertForTokenClassification extends ObjectMarshaller[BertForTokenClassification] {

  def unmarshal(jsonObj: Json): Either[Throwable, BertForTokenClassification] =
    jsonObj.as[BertForTokenClassification]
}

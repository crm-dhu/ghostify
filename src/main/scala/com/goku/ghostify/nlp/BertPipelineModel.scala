package com.goku.ghostify.nlp

import scala.collection.JavaConverters._

import com.goku.ghostify.common._
import com.goku.ghostify.tensorflow.{ModelSignatureConstants, ModelSignatureManager, TensorResources, TensorflowWrapper}
import org.tensorflow.ndarray.buffer.IntDataBuffer

class BertPipelineModel(
  tensorflowWrapper: TensorflowWrapper,
  vocabulary: Map[String, Int],
  id2labels: Map[Int, String],
  signatures: Option[Map[String, String]] = None,
  batchSize: Int = 8,
  maxSentenceLength: Int = 128,
  caseSensitive: Boolean = true
) extends Serializable {

  val _tfBertSignatures: Map[String, String] = signatures.getOrElse(ModelSignatureManager.apply())

  val bertPreprocessor = new BertPreprocessor(vocabulary, maxSentenceLength, caseSensitive)

  def predict(
    texts: Seq[String]
  ): Seq[String] = {
    val sentences = texts.zipWithIndex.map { case (r, id) => Sentence(r, 0, r.length - 1, id) }
    val encodedInput = bertPreprocessor.encode(sentences)
    val nerTagged = encodedInput
      .grouped(batchSize)
      .flatMap { batch =>
        val tokenIds = batch.map(_.wordpieceTokens.map(_.pieceId))
        val logits = tag(tokenIds)
        val results = batch.zip(logits)
        results.map { case (input, output) =>
          val tokenLen = input.tokenLength
          val inputTokens = input.wordpieceTokens.slice(1, tokenLen + 1)
          val outputScore = output.slice(1, tokenLen + 1).map { scores =>
            val (score, labelId) = scores.zipWithIndex.maxBy(_._1)
            (score, id2labels.getOrElse(labelId, "NA"))
          }
          inputTokens.zip(outputScore).map { case (tok, (score, label)) =>
            NerTagged(
              tok.wordpiece,
              tok.token,
              label,
              score,
              tok.isWordStart,
              tok.begin,
              tok.end
            )
          }
        }
      }
      .toSeq
    nerTagged.zip(sentences).map { case (tags, sentence) =>
      val nerMerged = NerPostProcessor.decode(tags.toSeq)
      val begin = 0 +: nerMerged.map(_.end + 1)
      val end = nerMerged.map(_.begin) :+ sentence.content.length
      val tag = nerMerged.map(_.tag) :+ ""
      begin
        .zip(end)
        .zip(tag)
        .map { case ((b, e), t) =>
          if (t.isEmpty) sentence.content.substring(b, e)
          else s"${sentence.content.substring(b, e)}[$t]"
        }
        .mkString("")
    }
  }

  def tag(batch: Seq[Array[Int]]): Seq[Array[Array[Float]]] = {
    val tensors = new TensorResources()

    val maxSentenceLength = batch.map(encodedSentence => encodedSentence.length).max
    val batchLength = batch.length

    val tokenBuffers: IntDataBuffer = tensors.createIntBuffer(batchLength * maxSentenceLength)
    val maskBuffers: IntDataBuffer = tensors.createIntBuffer(batchLength * maxSentenceLength)
    val segmentBuffers: IntDataBuffer = tensors.createIntBuffer(batchLength * maxSentenceLength)

    val shape = Array(batch.length.toLong, maxSentenceLength)

    batch.zipWithIndex
      .foreach { case (sentence, idx) =>
        val offset = idx * maxSentenceLength
        tokenBuffers.offset(offset).write(sentence)
        maskBuffers.offset(offset).write(sentence.map(x => if (x == 0) 0 else 1))
        segmentBuffers.offset(offset).write(Array.fill(maxSentenceLength)(0))
      }

    val session = tensorflowWrapper.getTFSessionWithSignature(
      configProtoBytes = None,
      savedSignatures = signatures,
      initAllTables = false
    )
    val runner = session.runner

    val tokenTensors = tensors.createIntBufferTensor(shape, tokenBuffers)
    val maskTensors = tensors.createIntBufferTensor(shape, maskBuffers)
    val segmentTensors = tensors.createIntBufferTensor(shape, segmentBuffers)

    runner
      .feed(
        _tfBertSignatures.getOrElse(ModelSignatureConstants.InputIds.key, "missing_input_id_key"),
        tokenTensors
      )
      .feed(
        _tfBertSignatures
          .getOrElse(ModelSignatureConstants.AttentionMask.key, "missing_input_mask_key"),
        maskTensors
      )
      .feed(
        _tfBertSignatures
          .getOrElse(ModelSignatureConstants.TokenTypeIds.key, "missing_segment_ids_key"),
        segmentTensors
      )
      .fetch(
        _tfBertSignatures
          .getOrElse(ModelSignatureConstants.LogitsOutput.key, "missing_logits_key")
      )

    val outs = runner.run().asScala
    val rawScores = TensorResources.extractFloats(outs.head)

    outs.foreach(_.close())
    tensors.clearSession(outs)
    tensors.clearTensors()

    val dim = rawScores.length / (batchLength * maxSentenceLength)
    val batchScores: Array[Array[Array[Float]]] = rawScores
      .grouped(dim)
      .map(scores => calculateSoftmax(scores))
      .toArray
      .grouped(maxSentenceLength)
      .toArray

    batchScores
  }

  def findIndexedToken(
    tokenizedSentences: Seq[TokenizedSentence],
    sentence: (WordpieceTokenizedSentence, Int),
    tokenPiece: TokenPiece
  ): Option[IndexedToken] = {
    tokenizedSentences(sentence._2).indexedTokens.find(p => p.begin == tokenPiece.begin)
  }

  def calculateSoftmax(scores: Array[Float]): Array[Float] = {
    val exp = scores.map(x => math.exp(x))
    exp.map(x => x / exp.sum).map(_.toFloat)
  }

}

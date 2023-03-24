package com.goku.ghostify.nlp

import com.goku.ghostify.PortalUnaryTransformer
import com.goku.ghostify.data.NamedFeature
import com.goku.ghostify.util.ObjectMarshaller
import com.johnsnowlabs.nlp.Annotation
import com.johnsnowlabs.nlp.annotators.common.{TokenPieceEmbeddings, TokenizedWithSentence, WordpieceEmbeddingsSentence}
import com.johnsnowlabs.nlp.embeddings.{HasEmbeddingsProperties, ReadsFromBytes, WordEmbeddingsReader}
import com.johnsnowlabs.storage.{Database, HasStorageModel, RocksDBConnection}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

case class WordEmbeddingsModel(
  inputCol: NamedFeature[Array[Annotation]],
  outputCol: NamedFeature[Array[Annotation]]
) extends PortalUnaryTransformer[Array[Annotation], Array[Annotation]] with HasEmbeddingsProperties
    with HasStorageModel with ReadsFromBytes {

  override def transformFeature(annotations: Array[Annotation]): Array[Annotation] = {
    val sentences = TokenizedWithSentence.unpack(annotations)
    val withEmbeddings = sentences.map { s =>
      val tokens = s.indexedTokens.map { indexedToken =>
        val (embeddings, zeroArray) = retrieveEmbeddings(indexedToken.token)
        TokenPieceEmbeddings(
          indexedToken.token,
          indexedToken.token,
          -1,
          isWordStart = true,
          embeddings,
          zeroArray,
          indexedToken.begin,
          indexedToken.end
        )
      }
      WordpieceEmbeddingsSentence(tokens, s.sentenceIndex)
    }

    WordpieceEmbeddingsSentence.pack(withEmbeddings).toArray
  }

  def retrieveEmbeddings(token: String): (Option[Array[Float]], Array[Float]) = {
    val storageReader = getReader(Database.EMBEDDINGS)
    val embeddings = storageReader.lookup(token)
    val zeroArray: Array[Float] = storageReader.emptyValue
    (embeddings, zeroArray)
  }

  private def bufferSizeFormula: Int = {
    scala.math
      .min( // LRU Cache Size, pick the smallest value up to 50k to reduce memory blue print as dimension grows
        (100.0 / $(dimension)) * 200000,
        50000
      )
      .toInt
  }
  override protected def createReader(
    database: Database.Name,
    connection: RocksDBConnection
  ): WordEmbeddingsReader = {
    new WordEmbeddingsReader(connection, $(caseSensitive), $(dimension), bufferSizeFormula)
  }

  override val databases: Array[Database.Name] = Array(Database.EMBEDDINGS)
  def marshal = this.asJson

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
//
  override val uid: String = s"WordEmbeddings: ${Identifiable.randomUID("WordEmbeddings")}"
}

object WordEmbeddingsModel extends ObjectMarshaller[WordEmbeddingsModel] {

  def unmarshal(jsonObj: Json): Either[Throwable, WordEmbeddingsModel] =
    jsonObj.as[WordEmbeddingsModel]
}

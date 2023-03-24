package com.goku.ghostify.core

import scala.util.Try

import com.goku.ghostify.PortalUnaryTransformer
import com.goku.ghostify.data.NamedFeature
import com.goku.ghostify.util.ObjectMarshaller
import com.johnsnowlabs.nlp.Annotation
import com.johnsnowlabs.nlp.AnnotatorType.DOCUMENT
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._

case class DocumentAssembler(
  inputCol: NamedFeature[String],
  outputCol: NamedFeature[Array[Annotation]]
) extends PortalUnaryTransformer[String, Array[Annotation]] {

  override def transformFeature(input: String): Array[Annotation] =
    assemble(input, Map("sentence" -> "0"))

  def assemble(text: String, metadata: Map[String, String]): Array[Annotation] = {
    val _text = Option(text).getOrElse("")
    Try(Array(Annotation(DOCUMENT, 0, _text.length - 1, _text, metadata))).toOption
      .getOrElse(Array.empty[Annotation])
  }
  def marshal = this.asJson
}

object DocumentAssembler extends ObjectMarshaller[DocumentAssembler] {

  def unmarshal(jsonObj: Json): Either[Throwable, DocumentAssembler] = jsonObj.as[DocumentAssembler]
}

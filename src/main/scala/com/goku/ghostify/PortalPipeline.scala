package com.goku.ghostify

import scala.collection.mutable.Builder

import com.goku.ghostify.data.FeatureMap
import com.goku.ghostify.util.ObjectMarshaller
import io.circe.syntax._
import io.circe.parser._

case class PortalPipeline(stages: Seq[PortalTransformer]) {

  def transform(featureMap: FeatureMap): FeatureMap =
    stages.foldLeft(featureMap)((m, t) => t.transform(m))

  def marshal: String = stages.map(ObjectMarshaller.marshal(_)).asJson.noSpaces

}

object PortalPipeline {

  def unmarshal(rawString: String): Either[Throwable, PortalPipeline] = {

    type BuilderEither = Either[Throwable, Builder[PortalTransformer, Seq[PortalTransformer]]]

    for {
      parsed <- parse(rawString)
      marshalledStages <- parsed.asArray.toRight(new Exception("stage is not an array"))
      stagesEither =
        marshalledStages
          .map { p => ObjectMarshaller.unmarshal[PortalTransformer](p) }
          .foldLeft(Right(Seq.newBuilder): BuilderEither) {
            case (Left(exp), _) => Left(exp)
            case (Right(builder), Right(t)) => Right(builder += t)
            case (Right(builder), Left(exp)) => Left(exp)
          }
      stages <- stagesEither
    } yield new PortalPipeline(stages.result())

  }

}

package com.goku.ghostify

import com.goku.ghostify.data.{FeatureMap, NamedFeature}
import io.circe.{Encoder, Json}

trait PortalTransformer {

  def transform(inputFeatures: FeatureMap): FeatureMap

  def marshal: Json

}

object PortalTransformer {

  implicit val encodePortalTransformer: Encoder[PortalTransformer] =
    new Encoder[PortalTransformer] {
      final def apply(x: PortalTransformer): Json = x.marshal
    }

}

trait PortalUnaryTransformer[I, O] extends PortalTransformer {

  def inputCol: NamedFeature[I]

  def outputCol: NamedFeature[O]

  def transformFeature(input: I): O

  override final def transform(inputFeatureMap: FeatureMap): FeatureMap = {
    inputFeatureMap.get(inputCol).fold(inputFeatureMap) { inputValue =>
      inputFeatureMap.put(outputCol, transformFeature(inputValue))
    }
  }

}

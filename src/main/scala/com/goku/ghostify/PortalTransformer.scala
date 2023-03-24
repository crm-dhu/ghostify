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

trait PortalBinaryTransformer[I1, I2, O] extends PortalTransformer {

  def inputCols: (NamedFeature[I1], NamedFeature[I2])

  def outputCol: NamedFeature[O]

  def transformFeature(inputs: (I1, I2)): O

  override final def transform(inputFeatureMap: FeatureMap): FeatureMap = {
    val (col1, col2) = inputCols
    val inputs = for {
      i1 <- inputFeatureMap.get(col1)
      i2 <- inputFeatureMap.get(col2)
    } yield (i1, i2)
    inputs.fold(inputFeatureMap) { inputValues =>
      inputFeatureMap.put(outputCol, transformFeature(inputValues))
    }
  }
}

trait PortalTernaryTransformer[I1, I2, I3, O] extends PortalTransformer {

  def inputCols: (NamedFeature[I1], NamedFeature[I2], NamedFeature[I3])

  def outputCol: NamedFeature[O]

  def transformFeature(inputs: (I1, I2, I3)): O

  override final def transform(inputFeatureMap: FeatureMap): FeatureMap = {
    val (col1, col2, col3) = inputCols
    val inputs = for {
      i1 <- inputFeatureMap.get(col1)
      i2 <- inputFeatureMap.get(col2)
      i3 <- inputFeatureMap.get(col3)
    } yield (i1, i2, i3)
    inputs.fold(inputFeatureMap) { inputValues =>
      inputFeatureMap.put(outputCol, transformFeature(inputValues))
    }
  }
}

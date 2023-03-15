package com.salesforce.mce.ghostify.nlp

trait HasOutputAnnotatorType {
  type AnnotatorType = String
  val outputAnnotatorType: AnnotatorType
}

package com.salesforce.mce.ghostify

object Params {

  final val InputCol = "text"
  final val EmailRegex = "[\\w-\\._]+@([\\w-]+\\.)+[\\w-]{2,4}".r
//  final val ModelPath = "/Users/donglin.hu/Workspace/ghostify/utils/models/dslim/bert-base-NER/saved_model/1"
  final val ModelPath = "s3a://mce-datapipeline-amusing-rattler-usw2-qa-common/artifacts/ghostify/bert-base-NER/saved_model/1"
}

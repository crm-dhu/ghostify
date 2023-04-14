package com.goku.ghostify.util

case class ExternalResource(path: String, readAs: ReadAs.Value, options: Map[String, String]) {

  if (readAs == ReadAs.SPARK)
    require(
      options.contains("format"),
      "Created ExternalResource to read as SPARK but key 'format' " +
        "in options is not provided. Can be any spark.read.format type. e.g. 'text' or 'json' or 'parquet'"
    )

}

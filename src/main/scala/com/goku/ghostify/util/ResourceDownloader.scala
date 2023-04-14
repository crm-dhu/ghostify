package com.goku.ghostify.util

import java.io.File
import java.net.URI
import java.nio.file.Paths

import org.apache.spark.SparkFiles

object ResourceDownloader {

  def downloadS3Directory(path: String): URI = {

    val (bucketName, keyPrefix) = ResourceHelper.parseS3URI(path)

    val awsGateway = new AWSGateway()
    val tmpDirectory = SparkFiles.getRootDirectory()
    awsGateway.downloadFilesFromDirectory(bucketName, keyPrefix, new File(tmpDirectory))

    Paths.get(tmpDirectory, keyPrefix).toUri
  }

}

package com.goku.ghostify.util

import java.io.File

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSStaticCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{S3Object, S3ObjectSummary}
import com.amazonaws.services.s3.transfer.{Transfer, TransferManagerBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.{AmazonClientException, AmazonServiceException}
import org.slf4j.{Logger, LoggerFactory}

class AWSGateway() extends AutoCloseable {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass.toString)

  lazy val client: AmazonS3 = {
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials
    AmazonS3ClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .withRegion(Regions.US_WEST_2)
      .build()
  }

  def downloadFilesFromDirectory(
    bucketName: String,
    keyPrefix: String,
    directoryPath: File
  ): Unit = {
    val transferManager = TransferManagerBuilder
      .standard()
      .withS3Client(client)
      .build()
    try {
      val multipleFileDownload =
        transferManager.downloadDirectory(bucketName, keyPrefix, directoryPath)
      println(multipleFileDownload.getDescription)
      waitForCompletion(multipleFileDownload)
    } catch {
      case e: AmazonServiceException =>
        throw new AmazonServiceException(
          "Amazon service error when downloading files from S3 directory: " + e.getMessage
        )
    }
    transferManager.shutdownNow()
  }

  private def waitForCompletion(transfer: Transfer): Unit = {
    try transfer.waitForCompletion()
    catch {
      case e: AmazonServiceException =>
        throw new AmazonServiceException("Amazon service error: " + e.getMessage)
      case e: AmazonClientException =>
        throw new AmazonClientException("Amazon client error: " + e.getMessage)
      case e: InterruptedException =>
        throw new InterruptedException("Transfer interrupted: " + e.getMessage)
    }
  }

  def getS3Object(bucket: String, s3FilePath: String): S3Object = {
    val s3Object = client.getObject(bucket, s3FilePath)
    s3Object
  }

  def listS3Files(bucket: String, s3Path: String): Array[S3ObjectSummary] = {
    try {
      val listObjects = client.listObjectsV2(bucket, s3Path)
      listObjects.getObjectSummaries.asScala.toArray
    } catch {
      case e: AmazonServiceException =>
        throw new AmazonServiceException("Amazon service error: " + e.getMessage)
      case NonFatal(unexpectedException) =>
        val methodName = Thread.currentThread.getStackTrace()(1).getMethodName
        throw new Exception(
          s"Unexpected error in ${this.getClass.getName}.$methodName: $unexpectedException"
        )
    }
  }

  override def close(): Unit = {
    client.shutdown()
  }

}

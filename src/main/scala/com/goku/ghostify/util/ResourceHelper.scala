package com.goku.ghostify.util

import java.io._
import java.net.URI
import java.nio.file
import java.nio.file.{Files, Paths}

import scala.collection.mutable.ArrayBuffer
import scala.io.BufferedSource

import com.amazonaws.AmazonServiceException
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object ResourceHelper {

  def getActiveSparkSession: SparkSession =
    SparkSession.getActiveSession.getOrElse(
      SparkSession
        .builder()
        .appName("SparkNLP Default Session")
        .master("local[*]")
        .config("spark.driver.memory", "22G")
        .config("spark.driver.maxResultSize", "0")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "1000m")
        .getOrCreate()
    )

  lazy val spark: SparkSession = getActiveSparkSession

  case class SourceStream(resource: String) {

    var fileSystem: Option[FileSystem] = None
    private val (pathExists, path) = OutputHelper.doesPathExists(resource)
    if (!pathExists) {
      throw new FileNotFoundException(s"file or folder: $resource not found")
    } else {
      fileSystem = Some(OutputHelper.getFileSystem(resource))
    }

    val pipe: Seq[InputStream] = getPipe(fileSystem.get)
    private val openBuffers: Seq[BufferedSource] = pipe.map(pp => {
      new BufferedSource(pp)("UTF-8")
    })
    val content: Seq[Iterator[String]] = openBuffers.map(c => c.getLines())

    private def getPipe(fileSystem: FileSystem): Seq[InputStream] = {
      if (fileSystem.getScheme == "s3a") {
        val awsGateway = new AWSGateway()
        val (bucket, s3Path) = parseS3URI(path.get.toString)
        val inputStreams = awsGateway.listS3Files(bucket, s3Path).map { summary =>
          val s3Object = awsGateway.getS3Object(bucket, summary.getKey)
          s3Object.getObjectContent
        }
        inputStreams
      } else {
        val files = fileSystem.listFiles(path.get, true)
        val buffer = ArrayBuffer.empty[InputStream]
        while (files.hasNext) buffer.append(fileSystem.open(files.next().getPath))
        buffer
      }
    }

    def copyToLocal(prefix: String = "sparknlp_tmp_"): URI = {
      if (fileSystem.get.getScheme == "file")
        return URI.create(resource)

      val destination: file.Path = Files.createTempDirectory(prefix)

      val destinationUri = fileSystem.get.getScheme match {
        case "hdfs" =>
          fileSystem.get.copyToLocalFile(false, path.get, new Path(destination.toUri), true)
          if (fileSystem.get.getFileStatus(path.get).isDirectory)
            Paths.get(destination.toString, path.get.getName).toUri
          else destination.toUri
        case "dbfs" =>
          val dbfsPath = path.toString.replace("dbfs:/", "/dbfs/")
          val sourceFile = new File(dbfsPath)
          val targetFile = new File(destination.toString)
          if (sourceFile.isFile) FileUtils.copyFileToDirectory(sourceFile, targetFile)
          else FileUtils.copyDirectory(sourceFile, targetFile)
          targetFile.toURI
        case _ =>
          val files = fileSystem.get.listFiles(path.get, false)
          while (files.hasNext) {
            fileSystem.get.copyFromLocalFile(files.next.getPath, new Path(destination.toUri))
          }
          destination.toUri
      }

      destinationUri
    }

    def close(): Unit = {
      openBuffers.foreach(_.close())
      pipe.foreach(_.close)
    }
  }

  def copyToLocal(path: String): String =
    try {
      val localUri =
        if (path.startsWith("s3:/") || path.startsWith("s3a:/")) { // Download directly from S3
          ResourceDownloader.downloadS3Directory(path)
        } else { // Use Source Stream
          val pathWithProtocol: String =
            if (URI.create(path).getScheme == null) new File(path).toURI.toURL.toString else path
          val resource = SourceStream(pathWithProtocol)
          resource.copyToLocal()
        }

      new File(localUri).getAbsolutePath // Platform independent path
    } catch {
      case awsE: AmazonServiceException =>
        println(
          "Error while retrieving folder from S3. Make sure you have set the right " +
            "access keys with proper permissions in your configuration. For an example please see " +
            "https://github.com/JohnSnowLabs/spark-nlp/blob/master/examples/python/training/english/dl-ner/mfa_ner_graphs_s3.ipynb"
        )
        throw awsE
      case e: Exception =>
        val copyToLocalErrorMessage: String =
          "Please make sure the provided path exists and is accessible while keeping in mind only file:/, hdfs:/, dbfs:/ and s3:/ protocols are supported at the moment."
        println(
          s"$e \n Therefore, could not create temporary local directory for provided path $path. $copyToLocalErrorMessage"
        )
        throw e
    }

  def parseLines(er: ExternalResource): Array[String] = {
    er.readAs match {
      case ReadAs.TEXT =>
        val sourceStream = SourceStream(er.path)
        val res = sourceStream.content.flatten.toArray
        sourceStream.close()
        res
      case ReadAs.SPARK =>
        import spark.implicits._
        spark.read
          .options(er.options)
          .format(er.options("format"))
          .load(er.path)
          .as[String]
          .collect
      case _ =>
        throw new Exception("Unsupported readAs")
    }
  }

  def parseS3URI(s3URI: String): (String, String) = {
    val prefix = if (s3URI.startsWith("s3:")) "s3://" else "s3a://"
    val bucketName = s3URI.substring(prefix.length).split("/").head
    val key = s3URI.substring((prefix + bucketName).length + 1)

    require(bucketName.nonEmpty, "S3 bucket name is empty!")

    (bucketName, key)
  }

}

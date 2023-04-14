package com.goku.ghostify.util

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.{FileSystem, Path}

object OutputHelper {

  private lazy val sparkSession = ResourceHelper.spark

  def getFileSystem: FileSystem = {
    FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
  }
  def getFileSystem(resource: String): FileSystem = {
    val resourcePath = new Path(parsePath(resource))
    FileSystem.get(resourcePath.toUri, sparkSession.sparkContext.hadoopConfiguration)
  }

  def parsePath(path: String): String = {
    val pathPrefix = path.split("://").head
    pathPrefix match {
      case "s3" => path.replace("s3", "s3a")
      case "file" => {
        val pattern = """^file:(/+)""".r
        pattern.replaceAllIn(path, "file:///")
      }
      case _ => path
    }
  }

  def doesPathExists(resource: String): (Boolean, Option[Path]) = {
    val fileSystem = OutputHelper.getFileSystem(resource)
    var modifiedPath = resource

    fileSystem.getScheme match {
      case "file" =>
        val path = new Path(resource)
        var exists = Try {
          fileSystem.exists(path)
        } match {
          case Success(value) => value
          case Failure(_) => false
        }

        if (!exists) {
          modifiedPath = resource.replaceFirst("//+", "///")
          exists = Try {
            fileSystem.exists(new Path(modifiedPath))
          } match {
            case Success(value) => value
            case Failure(_) => false
          }
        }

        if (!exists) {
          modifiedPath = resource.replaceFirst("/+", "//")
          exists = Try {
            fileSystem.exists(new Path(modifiedPath))
          } match {
            case Success(value) => value
            case Failure(_) => false
          }
        }

        if (!exists) {
          val pattern = """^file:/*""".r
          modifiedPath = pattern.replaceAllIn(resource, "")
          exists = Try {
            fileSystem.exists(new Path(modifiedPath))
          } match {
            case Success(value) => value
            case Failure(_) => false
          }
        }

        if (exists) {
          (exists, Some(new Path(modifiedPath)))
        } else (exists, None)
      case _ =>
        val exists = Try {
          val modifiedPath = parsePath(resource)
          fileSystem.exists(new Path(modifiedPath))
        } match {
          case Success(value) => value
          case Failure(_) => false
        }

        if (exists) {
          (exists, Some(new Path(modifiedPath)))
        } else (exists, None)
    }
  }
}

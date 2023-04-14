package com.goku.ghostify.tensorflow

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.tensorflow.Tensor
import org.tensorflow.ndarray.buffer._
import org.tensorflow.ndarray.{Shape, StdArrays}
import org.tensorflow.types._

class TensorResources {
  private val tensors = ArrayBuffer[Tensor]()

  def createTensor[T](obj: T): Tensor = {
    val result = obj match {
      case float: Float =>
        TFloat32.scalarOf(float)

      case int: Int =>
        TInt32.scalarOf(int)

      case boolean: Boolean =>
        TBool.scalarOf(boolean)

      case str: String =>
        TString.scalarOf(str)

      case array: Array[String] =>
        TString.tensorOf(StdArrays.ndCopyOf(array))

      case floatArray: Array[Float] =>
        TFloat32.tensorOf(StdArrays.ndCopyOf(floatArray))

      case bidimArray: Array[Array[String]] =>
        TString.tensorOf(StdArrays.ndCopyOf(bidimArray))

      case bidimArray: Array[Array[Float]] =>
        TFloat32.tensorOf(StdArrays.ndCopyOf(bidimArray))

      case tridimArray: Array[Array[Array[Float]]] =>
        TFloat32.tensorOf(StdArrays.ndCopyOf(tridimArray))

      case quaddimArray: Array[Array[Array[Array[Float]]]] =>
        TFloat32.tensorOf(StdArrays.ndCopyOf(quaddimArray))

      case array: Array[Int] =>
        TInt32.tensorOf(StdArrays.ndCopyOf(array))

      case array: Array[Array[Int]] =>
        TInt32.tensorOf(StdArrays.ndCopyOf(array))

      case array: Array[Array[Array[Int]]] =>
        TInt32.tensorOf(StdArrays.ndCopyOf(array))

      case array: Array[Array[Array[Byte]]] =>
        TUint8.tensorOf(StdArrays.ndCopyOf(array))

    }
    tensors.append(result)
    result
  }

  def createIntBufferTensor(shape: Array[Long], buf: IntDataBuffer): Tensor = {
    val result = TInt32.tensorOf(Shape.of(shape: _*), buf)
    tensors.append(result)
    result
  }

  def createLongBufferTensor(shape: Array[Long], buf: LongDataBuffer): Tensor = {
    val result = TInt64.tensorOf(Shape.of(shape: _*), buf)
    tensors.append(result)
    result
  }

  def createFloatBufferTensor(shape: Array[Long], buf: FloatDataBuffer): Tensor = {
    val result = TFloat32.tensorOf(Shape.of(shape: _*), buf)
    tensors.append(result)
    result
  }

  def createBooleanBufferTensor(shape: Array[Long], buf: BooleanDataBuffer): Tensor = {
    val result = TBool.tensorOf(Shape.of(shape: _*), buf)
    tensors.append(result)
    result
  }

  def clearTensors(): Unit = {
    for (tensor <- tensors) {
      tensor.close()
    }

    tensors.clear()
  }

  def clearSession(outs: mutable.Buffer[Tensor]): Unit = {
    outs.foreach(_.close())
  }

  def createIntBuffer(dim: Int): IntDataBuffer = {
    DataBuffers.ofInts(dim)
  }

  def createLongBuffer(dim: Int): LongDataBuffer = {
    DataBuffers.ofLongs(dim)
  }

  def createFloatBuffer(dim: Int): FloatDataBuffer = {
    DataBuffers.ofFloats(dim)
  }

  def createBooleanBuffer(dim: Int): BooleanDataBuffer = {
    DataBuffers.ofBooleans(dim)
  }
}

object TensorResources {
  // TODO all these implementations are not tested

  def calculateTensorSize(source: Tensor, size: Option[Int]): Int = {
    size.getOrElse {
      // Calculate real size from tensor shape
      val shape = source.shape()
      shape.asArray.foldLeft(1L)(_ * _).toInt
    }
  }

  def extractInts(source: Tensor, size: Option[Int] = None): Array[Int] = {
    val realSize = calculateTensorSize(source, size)
    val buffer = Array.fill(realSize)(0)
    source.asRawTensor.data.asInts.read(buffer)
    buffer
  }

  def extractInt(source: Tensor, size: Option[Int] = None): Int =
    extractInts(source).head

  def extractLongs(source: Tensor, size: Option[Int] = None): Array[Long] = {
    val realSize = calculateTensorSize(source, size)
    val buffer = Array.fill(realSize)(0L)
    source.asRawTensor.data.asLongs.read(buffer)
    buffer
  }

  def extractFloats(source: Tensor, size: Option[Int] = None): Array[Float] = {
    val realSize = calculateTensorSize(source, size)
    val buffer = Array.fill(realSize)(0f)
    source.asRawTensor.data.asFloats.read(buffer)
    buffer
  }
}

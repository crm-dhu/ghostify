package com.goku.ghostify.tensorflow

import java.io._
import java.nio.file.{Files, Paths}
import java.util.UUID

import scala.util.{Failure, Success, Try}

import com.goku.ghostify.util.ChunkBytes
import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}
import org.tensorflow._
import org.tensorflow.exceptions.TensorFlowException
import org.tensorflow.proto.framework.{ConfigProto, GraphDef}

case class Variables(variables: Array[Array[Byte]], index: Array[Byte])

class TensorflowWrapper(var variables: Variables, var graph: Array[Byte]) extends Serializable {

  /** For Deserialization */
  def this() = {
    this(null, null)
  }

  // Important for serialization on none-kyro serializers
  @transient private var m_session: Session = _
//  @transient private val logger = LoggerFactory.getLogger("TensorflowWrapper")

  def getTFSession(configProtoBytes: Option[Array[Byte]] = None): Session = this.synchronized {

    if (m_session == null) {
      val t = new TensorResources()
      val config = configProtoBytes.getOrElse(TensorflowWrapper.TFSessionConfig)

      // save the binary data of variables to file - variables per se
      val path = Files.createTempDirectory(
        UUID.randomUUID().toString.takeRight(12) + TensorflowWrapper.TFVarsSuffix
      )
      val folder = path.toAbsolutePath.toString

      val varData = Paths.get(folder, TensorflowWrapper.VariablesPathValue)
      ChunkBytes.writeByteChunksInFile(varData, variables.variables)

      // save the binary data of variables to file - variables' index
      val varIdx = Paths.get(folder, TensorflowWrapper.VariablesIdxValue)
      Files.write(varIdx, variables.index)

      // import the graph
      val _graph = new Graph()
      _graph.importGraphDef(GraphDef.parseFrom(graph))

      // create the session and load the variables
      val session = new Session(_graph, ConfigProto.parseFrom(config))
      val variablesPath =
        Paths.get(folder, TensorflowWrapper.VariablesKey).toAbsolutePath.toString

      session.runner
        .addTarget(TensorflowWrapper.SaveRestoreAllOP)
        .feed(TensorflowWrapper.SaveConstOP, t.createTensor(variablesPath))
        .run()

      // delete variable files
      Files.delete(varData)
      Files.delete(varIdx)

      m_session = session
    }
    m_session
  }

  def getTFSessionWithSignature(
    configProtoBytes: Option[Array[Byte]] = None,
    initAllTables: Boolean = true,
    savedSignatures: Option[Map[String, String]]
  ): Session = this.synchronized {

    if (m_session == null) {
      val t = new TensorResources()
      val config = configProtoBytes.getOrElse(TensorflowWrapper.TFSessionConfig)

      // save the binary data of variables to file - variables per se
      val path = Files.createTempDirectory(
        UUID.randomUUID().toString.takeRight(12) + TensorflowWrapper.TFVarsSuffix
      )
      val folder = path.toAbsolutePath.toString
      val varData = Paths.get(folder, TensorflowWrapper.VariablesPathValue)
      ChunkBytes.writeByteChunksInFile(varData, variables.variables)

      // save the binary data of variables to file - variables' index
      val varIdx = Paths.get(folder, TensorflowWrapper.VariablesIdxValue)
      Files.write(varIdx, variables.index)

      // import the graph
      val g = new Graph()
      g.importGraphDef(GraphDef.parseFrom(graph))

      // create the session and load the variables
      val session = new Session(g, ConfigProto.parseFrom(config))

      /**
       * a workaround to fix the issue with '''asset_path_initializer''' suggested at
       * https://github.com/tensorflow/java/issues/434 until we export models natively and not
       * just the GraphDef
       */
      try {
        session.initialize()
      } catch {
        case _: Exception => println("detect asset_path_initializer")
      }
      TensorflowWrapper
        .processInitAllTableOp(
          initAllTables,
          t,
          session,
          folder,
          TensorflowWrapper.VariablesKey,
          savedSignatures = savedSignatures
        )

      // delete variable files
      Files.delete(varData)
      Files.delete(varIdx)

      m_session = session
    }
    m_session
  }

}

/** Companion object */
object TensorflowWrapper {
  private[TensorflowWrapper] val logger: Logger = LoggerFactory.getLogger("TensorflowWrapper")

  /** log_device_placement=True, allow_soft_placement=True, gpu_options.allow_growth=True */
  private final val TFSessionConfig: Array[Byte] = Array[Byte](50, 2, 32, 1, 56, 1)

  // Variables
  val VariablesKey = "variables"
  val VariablesPathValue = "variables.data-00000-of-00001"
  val VariablesIdxValue = "variables.index"

  // Operations
  val InitAllTableOP = "init_all_tables"
  val SaveRestoreAllOP = "save/restore_all"
  val SaveConstOP = "save/Const"
  val SaveControlDependenciesOP = "save/control_dependency"

  // Model
  val SavedModelPB = "saved_model.pb"

  // TF vars suffix folder
  val TFVarsSuffix = "_tf_vars"

  // size of bytes store in each chunk/array
  // (Integer.MAX_VALUE - 8) * BUFFER_SIZE = store over 2 Petabytes
  private val BUFFER_SIZE = 1024 * 1024

  /** Utility method to load the TF saved model bundle */
  def withSafeSavedModelBundleLoader(
    tags: Array[String],
    savedModelDir: String
  ): SavedModelBundle = {
    Try(SavedModelBundle.load(savedModelDir, tags: _*)) match {
      case Success(bundle) => bundle
      case Failure(s) =>
        throw new Exception(s"Could not retrieve the SavedModelBundle + ${s.printStackTrace()}")
    }
  }

  /** Utility method to load the TF saved model components from a provided bundle */
  private def unpackFromBundle(folder: String, model: SavedModelBundle) = {
    val graph = model.graph()
    val session = model.session()
    val varPath = Paths.get(folder, VariablesKey, VariablesPathValue)
    val idxPath = Paths.get(folder, VariablesKey, VariablesIdxValue)
    (graph, session, varPath, idxPath)
  }

  /** Utility method to process init all table operation key */
  private def processInitAllTableOp(
    initAllTables: Boolean,
    tensorResources: TensorResources,
    session: Session,
    variablesDir: String,
    variablesKey: String,
    savedSignatures: Option[Map[String, String]]
  ) = {

    val _tfSignatures: Map[String, String] =
      savedSignatures.getOrElse(ModelSignatureManager.apply())

    lazy val legacySessionRunner = session.runner
      .addTarget(SaveRestoreAllOP)
      .feed(
        SaveConstOP,
        tensorResources.createTensor(Paths.get(variablesDir, variablesKey).toString)
      )

    /**
     * addTarget operation is the result of '''saverDef.getRestoreOpName()''' feed operation is
     * the result of '''saverDef.getFilenameTensorName()'''
     */
    lazy val newSessionRunner = session.runner
      .addTarget(_tfSignatures.getOrElse("restoreOpName_", "StatefulPartitionedCall_2"))
      .feed(
        _tfSignatures.getOrElse("filenameTensorName_", "saver_filename"),
        tensorResources.createTensor(Paths.get(variablesDir, variablesKey).toString)
      )

    def runRestoreNewNoInit = {
      newSessionRunner.run()
    }

    def runRestoreNewInit = {
      newSessionRunner.addTarget(InitAllTableOP).run()
    }

    def runRestoreLegacyNoInit = {
      legacySessionRunner.run()
    }

    def runRestoreLegacyInit = {
      legacySessionRunner.addTarget(InitAllTableOP).run()
    }

    if (initAllTables) {
      Try(runRestoreLegacyInit) match {
        case Success(_) => logger.debug("Running restore legacy with init...")
        case Failure(_) => runRestoreNewInit
      }
    } else {
      Try(runRestoreLegacyNoInit) match {
        case Success(_) => logger.debug("Running restore legacy with no init...")
        case Failure(_) => runRestoreNewNoInit
      }
    }
  }

  /** Utility method to load a Graph from path */
  def readGraph(graphFile: String): Graph = {
    val graphBytesDef = FileUtils.readFileToByteArray(new File(graphFile))
    val graph = new Graph()
    try {
      graph.importGraphDef(GraphDef.parseFrom(graphBytesDef))
    } catch {
      case e: TensorFlowException if e.getMessage.contains("Op type not registered 'BlockLSTM'") =>
        throw new UnsupportedOperationException(
          "Spark NLP tried to load a TensorFlow Graph using Contrib module, but" +
            " failed to load it on this system. If you are on Windows, please follow the correct steps for setup: " +
            "https://github.com/JohnSnowLabs/spark-nlp/issues/1022" +
            s" If not the case, please report this issue. Original error message:\n\n${e.getMessage}"
        )
    }
    graph
  }

  def read(
    folder: String,
    tags: Array[String] = Array.empty[String],
    initAllTables: Boolean = false
  ): (TensorflowWrapper, Option[Map[String, String]]) = {

    val t = new TensorResources()

    // 1. Create tmp folder
    val tmpFolder = Files
      .createTempDirectory(UUID.randomUUID().toString.takeRight(12) + "_ner")
      .toAbsolutePath
      .toString

    // 2. Read file as SavedModelBundle
    val (graph, session, varPath, idxPath, signatures) = {
      val model: SavedModelBundle =
        withSafeSavedModelBundleLoader(tags = tags, savedModelDir = folder)
      val (graph, session, varPath, idxPath) = unpackFromBundle(folder, model)
      if (initAllTables) session.runner().addTarget(InitAllTableOP)

      // Extract saved model signatures
      val saverDef = model.metaGraphDef().getSaverDef
      val signatures = ModelSignatureManager.extractSignatures(model, saverDef)

      (graph, session, varPath, idxPath, signatures)
    }

    val varBytes = ChunkBytes.readFileInByteChunks(varPath, BUFFER_SIZE)
    val idxBytes = Files.readAllBytes(idxPath)

    // 3. Remove tmp folder
    delete(tmpFolder)
    t.clearTensors()
    val tfWrapper =
      new TensorflowWrapper(Variables(varBytes, idxBytes), graph.toGraphDef.toByteArray)
    tfWrapper.m_session = session
    (tfWrapper, signatures)
  }

  def delete(file: String, throwOnError: Boolean = false): Unit = {
    val f = new File(file)
    if (f.exists()) {
      try {
        if (f.isDirectory)
          FileUtils.deleteDirectory(f)
        else
          FileUtils.deleteQuietly(f)
      } catch {
        case e: Exception =>
          if (throwOnError)
            throw e
          else
            FileUtils.forceDeleteOnExit(f)
      }
    }
  }
}

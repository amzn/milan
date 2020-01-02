package com.amazon.milan.flink

import java.util.ServiceLoader

import com.amazon.milan.flink.api.FlinkApplicationExtension
import com.amazon.milan.flink.application.{FlinkDataSink, FlinkDataSource}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


object MilanFlinkConfiguration {
  lazy val instance: MilanFlinkConfiguration = this.initializeConfiguration()

  private val logger = Logger(LoggerFactory.getLogger(getClass))

  private def initializeConfiguration(): MilanFlinkConfiguration = {
    this.logger.info("Initializing Milan Flink configuration.")

    val extensionLoader = ServiceLoader.load(classOf[FlinkApplicationExtension])
    val extensions = extensionLoader.asScala.toList

    this.logger.info(s"Found ${extensions.length} extensions in service configuration: [${extensions.map(_.getClass.getTypeName).mkString(", ")}].")

    new MilanFlinkConfiguration(extensions)
  }
}


class MilanFlinkConfiguration(private var extensions: List[FlinkApplicationExtension]) {
  def this() {
    this(List.empty)
  }

  def registerExtension(extension: FlinkApplicationExtension): Unit = {
    if (!extensions.contains(extension)) {
      this.extensions = this.extensions :+ extension
    }
  }

  def getDataSourceClass(typeName: String): Option[Class[_ <: FlinkDataSource[_]]] = {
    this.extensions.flatMap(_.getFlinkDataSourceClass(typeName)).headOption
  }

  def getDataSinkClass(typeName: String): Option[Class[_ <: FlinkDataSink[_]]] = {
    this.extensions.flatMap(_.getFlinkDataSinkClass(typeName)).headOption
  }
}

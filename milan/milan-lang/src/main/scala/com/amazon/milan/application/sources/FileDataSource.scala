package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.dataformats.{DataInputFormat, JsonDataInputFormat}
import com.amazon.milan.serialization.DataFormatConfiguration
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A [[DataSource]] that reads a file or files from a location.
 *
 * @param path          The path to the file or files.
 *                      This can be any type of path that is supported by the file system plugins at runtime, for example s3,
 *                      hdfs, local paths, etc.
 * @param dataFormat    A [[DataInputFormat]] that controls how objects are read.
 * @param configuration The data source configuration.
 * @tparam T The type of objects produced by the data source.
 */
@JsonSerialize
@JsonDeserialize
class FileDataSource[T: TypeDescriptor](val path: String,
                                        val dataFormat: DataInputFormat[T],
                                        val configuration: FileDataSource.Configuration)
  extends DataSource[T] {

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  def this(path: String, dataFormat: DataInputFormat[T]) {
    this(path, dataFormat, FileDataSource.Configuration.default)
  }

  def this(path: String) {
    this(path, new JsonDataInputFormat[T](DataFormatConfiguration.default))
  }

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}


object FileDataSource {

  /**
   * Configuration parameters for file data sources.
   *
   * @param readMode Controls how the data source behaves after all of the initial files have been processed.
   */
  case class Configuration(readMode: ReadMode.ReadMode) {
    def withReadMode(readMode: ReadMode.ReadMode): Configuration =
      Configuration(readMode)
  }

  /**
   * Controls how a data source behaves after all of the initial existing files have been processed.
   */
  object ReadMode extends Enumeration {
    type ReadMode = Value

    /**
     * The files that exist at the time the data source is created are read, and then the data source stops.
     */
    val Once: ReadMode = Value

    /**
     * The location is continuously monitored for new files and will not stop until cancelled.
     */
    val Continuous: ReadMode = Value
  }

  object Configuration {
    val default: Configuration = {
      Configuration(ReadMode.Once)
    }
  }

}
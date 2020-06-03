package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.dataformats.{DataInputFormat, JsonDataInputFormat}
import com.amazon.milan.serialization.DataFormatConfiguration
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


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

  object ReadMode extends Enumeration {
    type ReadMode = Value

    val Once, Continuous = Value
  }

  case class Configuration(readMode: ReadMode.ReadMode) {
    def withReadMode(readMode: ReadMode.ReadMode): Configuration =
      Configuration(readMode)
  }

  object Configuration {
    val default: Configuration = {
      Configuration(ReadMode.Once)
    }
  }

}
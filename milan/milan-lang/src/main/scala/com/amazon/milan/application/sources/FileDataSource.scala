package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.dataformats.{DataFormat, JsonDataFormat}
import com.amazon.milan.serialization.DataFormatConfiguration
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


@JsonSerialize
@JsonDeserialize
class FileDataSource[T: TypeDescriptor](val path: String,
                                        val dataFormat: DataFormat[T])
  extends DataSource[T] {

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  def this(path: String) {
    this(path, new JsonDataFormat[T](DataFormatConfiguration.default))
  }

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }
}

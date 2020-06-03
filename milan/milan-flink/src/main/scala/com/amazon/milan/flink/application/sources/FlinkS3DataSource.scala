package com.amazon.milan.flink.application.sources

import com.amazon.milan.application.sources.FileDataSource
import com.amazon.milan.dataformats.DataInputFormat
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.flink.api.common.typeinfo.TypeInformation


@JsonDeserialize
class FlinkS3DataSource[T](path: String,
                           dataFormat: DataInputFormat[T],
                           configuration: FileDataSource.Configuration,
                           recordTypeInformation: TypeInformation[T])
  extends FlinkFileDataSource[T](path, dataFormat, configuration, recordTypeInformation) {

  @JsonCreator
  def this(path: String, dataFormat: DataInputFormat[T], configuration: FileDataSource.Configuration) = {
    this(path, dataFormat, configuration, null)
  }
}

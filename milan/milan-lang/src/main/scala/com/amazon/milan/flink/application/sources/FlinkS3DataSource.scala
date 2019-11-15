package com.amazon.milan.flink.application.sources

import com.amazon.milan.dataformats.DataFormat
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.flink.api.common.typeinfo.TypeInformation


@JsonDeserialize
class FlinkS3DataSource[T](path: String,
                           dataFormat: DataFormat[T],
                           recordTypeInformation: TypeInformation[T])
  extends FlinkFileDataSource[T](path, dataFormat, recordTypeInformation) {

  @JsonCreator
  def this(path: String, dataFormat: DataFormat[T]) = {
    this(path, dataFormat, null)
  }
}

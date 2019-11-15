package com.amazon.milan.application.sources

import com.amazon.milan.dataformats.DataFormat
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.commons.lang.StringUtils


@JsonSerialize
@JsonDeserialize
class S3DataSource[T: TypeDescriptor](bucket: String,
                                      key: String,
                                      dataFormat: DataFormat[T])
  extends FileDataSource[T](S3DataSource.getS3Url(bucket, key), dataFormat) {
}


object S3DataSource {
  def getS3Url(bucket: String, key: String): String =
    s"s3://${StringUtils.strip(bucket, "/")}/${StringUtils.strip(key, "/")}"
}

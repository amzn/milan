package com.amazon.milan.application.sources

import com.amazon.milan.dataformats.DataInputFormat
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.commons.lang.StringUtils


/**
 * A data source for data in S3.
 *
 * @param bucket        The name of the bucket.
 * @param key           The key of the item or items.
 *                      If the key is a folder then all objects in the folder are treated as part of the dataset.
 * @param dataFormat    A [[DataInputFormat]] that controls how objects are read.
 * @param configuration The data source configuration.
 * @tparam T The type of items produced by the data source.
 */
@JsonSerialize
@JsonDeserialize
class S3DataSource[T: TypeDescriptor](bucket: String,
                                      key: String,
                                      dataFormat: DataInputFormat[T],
                                      configuration: FileDataSource.Configuration)
  extends FileDataSource[T](S3DataSource.getS3Url(bucket, key), dataFormat, configuration) {

  def this(bucket: String, key: String, dataFormat: DataInputFormat[T]) {
    this(bucket, key, dataFormat, FileDataSource.Configuration.default)
  }
}


object S3DataSource {
  def getS3Url(bucket: String, key: String): String =
    s"s3://${StringUtils.strip(bucket, "/")}/${StringUtils.strip(key, "/")}"
}

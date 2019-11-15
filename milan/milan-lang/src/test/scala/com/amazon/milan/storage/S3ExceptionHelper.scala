package com.amazon.milan.storage

import com.amazonaws.services.s3.internal.AmazonS3ExceptionBuilder
import com.amazonaws.services.s3.model.AmazonS3Exception

object S3ExceptionHelper {
  def create(statusCode: Int): AmazonS3Exception = {
    val builder = new AmazonS3ExceptionBuilder()
    builder.setStatusCode(statusCode)
    builder.build()
  }
}

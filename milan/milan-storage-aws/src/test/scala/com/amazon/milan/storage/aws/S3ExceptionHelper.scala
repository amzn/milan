package com.amazon.milan.storage.aws

import software.amazon.awssdk.services.s3.model.S3Exception

object S3ExceptionHelper {
  def create(statusCode: Int): S3Exception = {
    S3Exception.builder().statusCode(statusCode).build().asInstanceOf[S3Exception]
  }
}

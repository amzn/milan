package com.amazon.milan.flink.api

import org.apache.flink.api.common.typeinfo.TypeInformation


trait AccumulatorTypeQueryable[T] {
  def getAccumulatorType: TypeInformation[T]
}

package com.amazon.milan.flink.application

import com.amazon.milan.Id
import com.amazon.milan.application.Application


/**
 * This class mirrors [[com.amazon.milan.application.ApplicationInstance]].
 * It has the same fields, but some of the field types are changed to the Milan-Flink versions of those types.
 * It can be deserialized from the JSON-serialized [[com.amazon.milan.application.ApplicationInstance]] to yield
 * an application instance object that can be used by the Milan Flink compiler.
 */
class FlinkApplicationInstance(val instanceDefinitionId: String,
                               val application: Application,
                               val config: FlinkApplicationConfiguration) {

  def this(application: Application, config: FlinkApplicationConfiguration) {
    this(Id.newId(), application, config)
  }
}

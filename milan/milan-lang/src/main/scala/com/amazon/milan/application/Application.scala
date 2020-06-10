package com.amazon.milan.application

import com.amazon.milan.lang.StreamGraph
import com.amazon.milan.{Id, SemanticVersion}


/**
 * Represents a Milan application.
 *
 * @param applicationId The unique ID of the application.
 * @param graph         The dataflow graph of the application.
 */
class Application(val applicationId: String,
                  val graph: StreamGraph,
                  val version: SemanticVersion) {

  def this(graph: StreamGraph) {
    this(Id.newId(), graph, SemanticVersion.ZERO)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: Application =>
      this.applicationId == o.applicationId &&
        this.graph.equals(o.graph) &&
        this.version.equals(o.version)

    case _ =>
      false
  }
}

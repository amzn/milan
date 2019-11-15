package com.amazon.milan.application

import com.amazon.milan.Id
import com.amazon.milan.lang.StreamGraph


class Application(val applicationId: String, val graph: StreamGraph) {
  def this(graph: StreamGraph) {
    this(Id.newId(), graph)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: Application =>
      this.applicationId == o.applicationId &&
        this.graph.equals(o.graph)

    case _ =>
      false
  }
}

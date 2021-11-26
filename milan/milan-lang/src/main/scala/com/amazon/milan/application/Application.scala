package com.amazon.milan.application

import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.{Id, SemanticVersion}


/**
 * Represents a Milan application.
 *
 * @param applicationId The unique ID of the application.
 * @param streams       The streams in the application.
 */
class Application(val applicationId: String,
                  val streams: StreamCollection,
                  val version: SemanticVersion) {

  def this(streams: StreamCollection) {
    this(Id.newId(), streams, SemanticVersion.ZERO)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: Application =>
      this.applicationId == o.applicationId &&
        this.streams.equals(o.streams) &&
        this.version.equals(o.version)

    case _ =>
      false
  }
}

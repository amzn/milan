package com.amazon.milan.compiler.scala.event

case class GeneratedStreams(streams: List[StreamInfo]) {
  def getStream(streamId: String): StreamInfo =
    streams.find(_.streamId == streamId).get
}

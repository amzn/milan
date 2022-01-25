package com.amazon.milan.compiler.scala.event

case class GeneratedStreams(streams: List[StreamInfo],
                            externalStreams: List[ExternalStreamConsumer]) {
  def getStream(streamId: String): StreamInfo =
    this.streams.find(_.streamId == streamId).get

  def getExternalStreamConsumer(streamId: String): ExternalStreamConsumer =
    this.externalStreams.find(_.streamId == streamId).get
}

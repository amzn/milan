package com.amazon.milan.compiler.scala.event

/**
 * Contains information about the generated code.
 *
 * @param className           The name of the generated class that contains the event handler.
 * @param propertiesClassName The name of the Properties class that is used for the event handler constructor.
 * @param properties          The properties of the Properties class.
 */
case class GeneratorOutputInfo(className: String,
                               propertiesClassName: String,
                               properties: List[ClassProperty],
                               streams: List[StreamInfo],
                               externalStreams: List[ExternalStreamConsumer],
                               stateStores: List[GeneratedStateStore]) {
  def getStream(streamId: String): StreamInfo =
    this.streams.find(_.streamId == streamId).get

  def getExternalStreamConsumer(streamId: String): ExternalStreamConsumer =
    this.externalStreams.find(_.streamId == streamId).get
}

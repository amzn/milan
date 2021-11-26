package com.amazon.milan.graph

import com.amazon.milan.lang.{Stream, field, fields}
import com.amazon.milan.serialization.MilanObjectMapper
import com.amazon.milan.test.{IntKeyValueRecord, IntRecord}
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.junit.Assert._
import org.junit.{After, Before, Test}


@Test
class TestStreamCollection {
  @Before
  def before(): Unit = {
    Configurator.setRootLevel(Level.DEBUG)
  }

  @After
  def after(): Unit = {
    Configurator.setRootLevel(Level.INFO)
  }

  @Test
  def test_StreamCollection_SerializeAndDeserialize_ReturnsEquivalentGraph(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(r => fields(field("r", r)))

    val streams = StreamCollection.build(mapped)

    val mapper = new MilanObjectMapper()
    val json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(streams)

    // Parse the json into an array, just to check the correct number of streams are in the json.
    val parsed = mapper.readValue[StreamCollection](json, classOf[StreamCollection])

    assertEquals(streams, parsed)
  }

  @Test
  def test_StreamCollection_GetDereferencedGraph_Then_GetStreams_WithOneStreamAddedWithOneInput_ReturnsBothStreamNodesWithoutReferences(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(r => fields(field("record", r)))

    val streams = StreamCollection.build(mapped)

    val graphStreams = streams.getDereferencedStreams.sortBy(_.nodeId)
    val originalStreams = List(input, mapped).map(_.expr).sortBy(_.nodeId)
    assertEquals(originalStreams, graphStreams)
  }

  @Test
  def test_StreamCollection_GetDereferencedGraph_Then_GetStreams_AfterDeserialization_WithOneStreamAddedWithOneInput_ReturnsBothStreamNodesWithoutReferences(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(r => fields(field("record", r)))

    val streams = StreamCollection.build(mapped)

    val mapper = new MilanObjectMapper()
    val json = mapper.writeValueAsString(streams)

    val parsedGraph = mapper.readValue[StreamCollection](json, classOf[StreamCollection])

    val graphStreams = parsedGraph.getDereferencedStreams.sortBy(_.nodeId)
    val originalStreams = List(input, mapped).map(_.expr).sortBy(_.nodeId)
    assertEquals(originalStreams, graphStreams)
  }

  @Test
  def test_StreamCollection_TypeCheckGraph_OfDeserializedGraph_WithFullJoinAndSelect_AssignsExpectedTypes(): Unit = {
    val left = Stream.of[IntKeyValueRecord]
    val right = Stream.of[IntKeyValueRecord]
    val joined = left.fullJoin(right).where((l, r) => l.key == r.key)
    val output = joined.select((l, r) => IntKeyValueRecord(l.key, l.value + r.value))

    val streams = StreamCollection.build(output)

    val streamsCopy = MilanObjectMapper.copy(streams)

    val leftCopy = streamsCopy.getStream(left.streamId)
    assertEquals(TypeDescriptor.streamOf[IntKeyValueRecord], leftCopy.tpe)

    val rightCopy = streamsCopy.getStream(right.streamId)
    assertEquals(TypeDescriptor.streamOf[IntKeyValueRecord], rightCopy.tpe)

    val outputCopy = streamsCopy.getStream(output.streamId)
    assertEquals(TypeDescriptor.streamOf[IntKeyValueRecord].fullName, outputCopy.tpe.fullName)
  }

  @Test
  def test_StreamCollection_TypeCheckGraph_OfNestedStreamFunction_DoesntFail(): Unit = {
    val input = Stream.of[IntKeyValueRecord]

    def maxByValue(stream: Stream[IntKeyValueRecord]): Stream[IntKeyValueRecord] = {
      stream.maxBy(r => r.value)
    }

    val output = input.groupBy(r => r.key).flatMap((key, group) => maxByValue(group))

    val streams = StreamCollection.build(output).getDereferencedStreams
    typeCheckGraph(streams)
  }
}

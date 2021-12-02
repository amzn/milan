package com.amazon.milan.compiler.scala.event

import com.amazon.milan.Id.newId
import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.scala.testing._
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang.Stream
import com.amazon.milan.lang.aggregation._
import org.junit.Assert._
import org.junit.Test

import java.time.Instant


object TestEventApp {
  case class SourceTableColumn(name: String, description: String)

  case class SourceTable(recordId: String, tableId: String, description: String, columns: List[SourceTableColumn]) {
    override def equals(obj: Any): Boolean = obj match {
      case SourceTable(_, id, desc, cols) => this.tableId.equals(id) && this.description.eq(desc) && this.columns.equals(cols)
      case _ => false
    }
  }

  case class AnnotationEvent(recordId: String, tableId: String, eventTime: Instant, target: String, data: String)

  case class TableAnnotationEvents(recordId: String, tableId: String, events: Map[String, AnnotationEvent])

  case class OutputTableColumn(name: String, description: String) {
    def withDescription(newDescription: String): OutputTableColumn =
      OutputTableColumn(this.name, newDescription)
  }

  case class OutputTable(recordId: String, tableId: String, description: String, columns: List[OutputTableColumn]) {
    def withDescription(newDescription: String): OutputTable =
      OutputTable(newId(), this.tableId, newDescription, this.columns)

    def withColumns(newColumns: List[OutputTableColumn]): OutputTable =
      OutputTable(newId(), this.tableId, this.description, newColumns)

    override def equals(obj: Any): Boolean = obj match {
      case OutputTable(_, id, desc, cols) => this.tableId.equals(id) && this.description.eq(desc) && this.columns.equals(cols)
      case _ => false
    }
  }

  def sourceTableToOutput(sourceTable: SourceTable): OutputTable = {
    val outputColumns = sourceTable.columns.map(column => OutputTableColumn(column.name, column.description))

    OutputTable(
      newId(),
      sourceTable.tableId,
      sourceTable.description,
      outputColumns
    )
  }

  def updateAnnotations(annotations: Option[TableAnnotationEvents], event: AnnotationEvent): (Option[TableAnnotationEvents], TableAnnotationEvents) = {
    val newAnnotations =
      annotations match {
        case None =>
          TableAnnotationEvents(newId(), event.tableId, Map(event.target -> event))

        case Some(a) =>
          TableAnnotationEvents(newId(), event.tableId, a.events + (event.target -> event))
      }

    (Some(newAnnotations), newAnnotations)
  }

  def applyAnnotations(outputTable: OutputTable, annotations: TableAnnotationEvents): OutputTable = {
    if (annotations == null) {
      outputTable
    }
    else {
      val recordWithDescription =
        annotations.events.get("TableDescription") match {
          case Some(annotation) => outputTable.withDescription(annotation.data)
          case None => outputTable
        }

      val columns = outputTable.columns.map(column => {
        annotations.events.get(s"ColumnDescription:${column.name}") match {
          case Some(annotation) => column.withDescription(annotation.data)
          case None => column
        }
      })

      recordWithDescription.withColumns(columns)
    }
  }
}

import com.amazon.milan.compiler.scala.event.TestEventApp._


@Test
class TestEventApp {
  @Test
  def test_EventApp_TableAnnotation(): Unit = {
    // In this test there is a source of data table metadata, and another source of annotation events that can override
    // that metadata.
    val sourceTables = Stream.of[SourceTable].withId("sourceTables")

    val annotationEvents = Stream.of[AnnotationEvent].withId("annotationEvents")
    val tableAnnotations = this.getAnnotationStream(annotationEvents)

    val sourceOutputTables = sourceTables.map(table => TestEventApp.sourceTableToOutput(table)).withId("sourceOutputTables")

    val outputTables =
      sourceOutputTables
        .fullJoin(tableAnnotations)
        .where((l, r) => l.tableId == r.tableId)
        .select((table, annotation) => TestEventApp.applyAnnotations(table, annotation))
        .withId("outputTables")

    val streams = StreamCollection.build(outputTables)
    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(outputTables)
    val tester = EventAppTester.compile(streams, config)

    val sourceTable = SourceTable(
      newId(),
      "tableId",
      "source desc",
      List(
        SourceTableColumn("a", "a source desc"),
        SourceTableColumn("b", "b source desc"),
      )
    )

    tester.consume(sourceTables.streamId, sourceTable)
    val output1 = sink.getValues.last
    assertEquals(TestEventApp.sourceTableToOutput(sourceTable), output1)

    val event1 = AnnotationEvent(newId(), "tableId", Instant.now(), "TableDescription", "event desc")
    tester.consume(annotationEvents.streamId, event1)
    val output2 = sink.getValues.last
    assertEquals("event desc", output2.description)
  }

  private def getAnnotationStream(userAnnotations: Stream[AnnotationEvent]): Stream[TableAnnotationEvents] = {
    def collectAnnotationsByTable(events: Stream[AnnotationEvent]): Stream[TableAnnotationEvents] = {
      events.scan(
        Option.empty[TableAnnotationEvents],
        (state: Option[TableAnnotationEvents], event) => TestEventApp.updateAnnotations(state, event)
      ).withId("collectAllAnnotations")
    }

    val annotationsByTable =
      userAnnotations
        .groupBy(r => r.tableId)
        .flatMap((tableId, annotations) => collectAnnotationsByTable(annotations)).withId("latestAnnotationsByTable")
        .withId("tableAnnotations")

    annotationsByTable
  }

  @Test
  def test_EventApp_AggregateInGroup(): Unit = {
    // This graph performs the following computation:
    // For every unique value of A, sum up the value of D
    // But only choose one value of D for each unique value of B for that A.
    // Choose that value of D by picking the record with the highest value of C
    // for that (A, B) combination.

    val input = Stream.of[FourIntRecord].withId("input")

    def maxByC(items: Stream[FourIntRecord]): Stream[FourIntRecord] = {
      items.maxBy(r => r.c).withId("maxItemsByC")
    }

    def sumDByLatestCForB(items: Stream[FourIntRecord]): Stream[TwoIntRecord] = {
      items
        .groupBy(r => r.b).withId("groupByB")
        .map((k, g) => maxByC(g)).withId("maxGroupsByC")
        .recordWindow(1).withId("recordWindow")
        .select(r => TwoIntRecord(any(r.a), sum(r.d))).withId("sumOfD")
    }

    val output =
      input
        .groupBy(r => r.a).withId("groupByA")
        .flatMap((k, g) => sumDByLatestCForB(g)).withId("output")

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(output)
    val tester = EventAppTester.compile(output, config)

    // Send some records with A = 1 to test the inner grouping and aggregation.
    tester.consume("input", FourIntRecord(1, 1, 1, 1))
    assertEquals(1, sink.getRecordCount)
    assertEquals(TwoIntRecord(1, 1), sink.getValues.last)

    tester.consume("input", FourIntRecord(1, 1, 2, 3))
    assertEquals(2, sink.getRecordCount)
    assertEquals(TwoIntRecord(1, 3), sink.getValues.last)

    tester.consume("input", FourIntRecord(1, 2, 1, 5))
    assertEquals(3, sink.getRecordCount)
    assertEquals(TwoIntRecord(1, 8), sink.getValues.last)

    tester.consume("input", FourIntRecord(1, 2, 2, 2))
    assertEquals(4, sink.getRecordCount)
    assertEquals(TwoIntRecord(1, 5), sink.getValues.last)

    // Do the same thing with A = 2 to test the outer grouping.
    tester.consume("input", FourIntRecord(2, 1, 1, 1))
    assertEquals(5, sink.getRecordCount)
    assertEquals(TwoIntRecord(2, 1), sink.getValues.last)

    tester.consume("input", FourIntRecord(2, 1, 2, 3))
    assertEquals(6, sink.getRecordCount)
    assertEquals(TwoIntRecord(2, 3), sink.getValues.last)

    tester.consume("input", FourIntRecord(2, 2, 1, 5))
    assertEquals(7, sink.getRecordCount)
    assertEquals(TwoIntRecord(2, 8), sink.getValues.last)

    tester.consume("input", FourIntRecord(2, 2, 2, 2))
    assertEquals(8, sink.getRecordCount)
    assertEquals(TwoIntRecord(2, 5), sink.getValues.last)
  }
}

package com.amazon.milan.aws.metrics

import org.junit.Assert._
import org.junit.Test


@Test
class TestCloudFormationJsonGenerators {
  @Test
  def test_dashboard__no_widgets(): Unit = {
    val dashboardJsonGenerator = new DashboardJsonGenerator
    assertEquals("{\n  \\\"widgets\\\": [\n\n    \n\n  ]\n}", dashboardJsonGenerator.toJson)
  }

  @Test
  def test_dashboard__single_widgets(): Unit = {
    val dashboardJsonGenerator = new DashboardJsonGenerator()
      .withWidget(new TestWidgetGenerator)

    assertEquals("{\n  \\\"widgets\\\": [\n\n    testjson\n\n  ]\n}", dashboardJsonGenerator.toJson)
  }

  @Test
  def test_dashboard__multiple_widgets(): Unit = {
    val dashboardJsonGenerator = new DashboardJsonGenerator()
      .withWidgets(List(new TestWidgetGenerator, new TestWidgetGenerator))

    assertEquals("{\n  \\\"widgets\\\": [\n\n    testjson,\ntestjson\n\n  ]\n}", dashboardJsonGenerator.toJson)
  }

  @Test
  def test_line_widget(): Unit = {
    val generatedJSON = new LineWidgetJsonGenerator(
      namespace = "ns",
      metricName = "mn",
      metricType = "mt",
      stacked = true,
      region = "reg",
      periodSec = 30,
      title = "tit",
      axisLabel = "lab",
      host = "hos",
      liveData = false
    ).toJson

    assertEquals(
      "{\n  \\\"type\\\": \\\"metric\\\",\n  \\\"x\\\": 0,\n  \\\"y\\\": 0,\n  \\\"width\\\": 6,\n  \\\"height\\\": 6,\n  \\\"properties\\\": {\n    \\\"metrics\\\": [\n      [ \\\"ns\\\", \\\"mn\\\", \\\"host\\\", \\\"hos\\\", \\\"metric_type\\\", \\\"mt\\\", { \\\"label\\\": \\\"p50\\\", \\\"stat\\\": \\\"p50\\\" } ],\n      [ \\\"...\\\", { \\\"stat\\\": \\\"p90\\\", \\\"label\\\": \\\"p90\\\" } ],\n      [ \\\"...\\\", { \\\"label\\\": \\\"p99\\\" } ]\n    ],\n    \\\"view\\\": \\\"timeSeries\\\",\n    \\\"stacked\\\": true,\n    \\\"region\\\": \\\"reg\\\",\n    \\\"liveData\\\": false,\n    \\\"stat\\\": \\\"p99\\\",\n    \\\"period\\\": 30,\n    \\\"title\\\": \\\"tit\\\",\n    \\\"yAxis\\\": {\n      \\\"left\\\": {\n        \\\"showUnits\\\": false,\n        \\\"label\\\": \\\"lab\\\"\n      },\n      \\\"right\\\": {\n        \\\"label\\\": \\\"\\\"\n      }\n    }\n  }\n}",
      generatedJSON
    )
  }

  @Test
  def test_text_widget(): Unit = {
    val generatedJSON = new TextWidgetJsonGenerator("hello").toJson

    assertEquals(
      "{\n  \\\"type\\\": \\\"text\\\",\n  \\\"width\\\": 6,\n  \\\"height\\\": 6,\n  \\\"properties\\\": {\n    \\\"markdown\\\": \\\"hello\\\"\n  }\n}",
      generatedJSON)
  }

  class TestWidgetGenerator extends WidgetJsonGenerator {
    override def toJson: String = "testjson"
  }

}

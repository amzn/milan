package com.amazon.milan.aws.metrics


/**
 * A [[CloudFormationJsonGenerator]] that can generate JSON for a line widget.
 */
class LineWidgetJsonGenerator(val namespace: String, val metricName: String, val metricType: String,
                              val stacked: Boolean, val region: String, val periodSec: Int,
                              val title: String, val axisLabel: String, val host: String,
                              val liveData: Boolean) extends WidgetJsonGenerator {

  override def toJson: String = {
    "{\n  \\\"type\\\": \\\"metric\\\",\n  \\\"x\\\": 0,\n  \\\"y\\\": 0,\n  \\\"width\\\": 6,\n  \\\"height\\\": 6,\n  \\\"properties\\\": {\n    \\\"metrics\\\": [\n      [ \\\"NAMESPACE\\\", \\\"METRICNAME\\\", \\\"host\\\", \\\"HOST\\\", \\\"metric_type\\\", \\\"METRICTYPE\\\", { \\\"label\\\": \\\"p50\\\", \\\"stat\\\": \\\"p50\\\" } ],\n      [ \\\"...\\\", { \\\"stat\\\": \\\"p90\\\", \\\"label\\\": \\\"p90\\\" } ],\n      [ \\\"...\\\", { \\\"label\\\": \\\"p99\\\" } ]\n    ],\n    \\\"view\\\": \\\"timeSeries\\\",\n    \\\"stacked\\\": STACKED,\n    \\\"region\\\": \\\"REGION\\\",\n    \\\"liveData\\\": LIVEDATA,\n    \\\"stat\\\": \\\"p99\\\",\n    \\\"period\\\": PERIODSEC,\n    \\\"title\\\": \\\"TITLE\\\",\n    \\\"yAxis\\\": {\n      \\\"left\\\": {\n        \\\"showUnits\\\": false,\n        \\\"label\\\": \\\"LABEL\\\"\n      },\n      \\\"right\\\": {\n        \\\"label\\\": \\\"\\\"\n      }\n    }\n  }\n}"
      .replace("NAMESPACE", this.namespace)
      .replace("METRICNAME", this.metricName)
      .replace("METRICTYPE", this.metricType)
      .replace("STACKED", this.stacked.toString)
      .replace("REGION", this.region)
      .replace("PERIODSEC", this.periodSec.toString)
      .replace("TITLE", this.title)
      .replace("LABEL", this.axisLabel)
      .replace("HOST", this.host)
      .replace("LIVEDATA", this.liveData.toString)
  }
}

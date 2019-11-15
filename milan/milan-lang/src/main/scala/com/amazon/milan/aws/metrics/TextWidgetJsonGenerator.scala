package com.amazon.milan.aws.metrics


/**
 * A [[CloudFormationJsonGenerator]] that can generate JSON for a text widget.
 */
class TextWidgetJsonGenerator(markdown: String) extends WidgetJsonGenerator {
  override def toJson: String = {
    "{\n  \\\"type\\\": \\\"text\\\",\n  \\\"width\\\": 6,\n  \\\"height\\\": 6,\n  \\\"properties\\\": {\n    \\\"markdown\\\": \\\"MARKDOWN\\\"\n  }\n}"
      .replace("MARKDOWN", this.markdown)
  }
}

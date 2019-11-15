package com.amazon.milan.aws.metrics

import scala.collection.mutable.ListBuffer


/**
 * A [[CloudFormationJsonGenerator]] that can generate JSON for a dashboard.
 */
class DashboardJsonGenerator extends CloudFormationJsonGenerator {
  private val widgets = new ListBuffer[WidgetJsonGenerator]()

  /**
   * Add widgets to dashboard.
   *
   * @param widgets List of widgets to add.
   * @return [[DashboardJsonGenerator]]
   */
  def withWidgets(widgets: List[WidgetJsonGenerator]): DashboardJsonGenerator = {
    widgets.foreach(w => this.withWidget(w))
    this
  }

  /**
   * Add widget to dashboard.
   *
   * @param widget Widget to add.
   * @return [[DashboardJsonGenerator]]
   */
  def withWidget(widget: WidgetJsonGenerator): DashboardJsonGenerator = {
    this.widgets += widget
    this
  }

  override def toJson: String = {
    "{\n  \\\"widgets\\\": [\n\n    WIDGETS\n\n  ]\n}"
      .replace("WIDGETS", this.widgets.map(w => w.toJson).mkString(",\n"))
  }
}

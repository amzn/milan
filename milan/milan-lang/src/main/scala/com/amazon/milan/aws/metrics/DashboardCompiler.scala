package com.amazon.milan.aws.metrics


object MetricType {
  val Counter = "Counter" // Keeps a running count of the number of records
  val Meter = "Meter" // Measures the one-minute exponentially-weighted moving average throughput
  val Histogram = "Histogram" // Calculates the distribution of a value
}


object DashboardCompiler {
  private val DASHBOARD_NAME_PLACEHOLDER = "DASHBOARDNAME"
  private val DASHBOARD_BODY_PLACEHOLDER = "DASHBOARDBODY"
  private val AUTODASH_TEXT = "This dashboard was created for Milan using CloudFormation and should not be changed in the console."

  /**
   * Compiles a list of compiled metrics into a CloudFormation Dashboard template.
   *
   * @param applicationInstanceId The ID of the instance that the dashboard is for.
   * @param compiledMetrics       List of compiled metrics.
   * @param workerHosts           List of IP addresses of the hosts that the job can be deployed to.
   * @param maxParallelism        The max-parallelism of the job.
   * @param region                The AWS region that the metrics are in.
   * @param namespace             The CloudWatch namespace of the metrics.
   * @param masterHost            The IP address of the master host that sends metrics to CloudWatch.
   * @return CloudFormation template for a Dashboard.
   */
  def compile(applicationInstanceId: String,
              compiledMetrics: List[CompiledMetric],
              workerHosts: List[String],
              maxParallelism: Int,
              region: String,
              namespace: String,
              masterHost: String): String = {
    val dashboardName = s"milan-dashboard-$applicationInstanceId"
    val dashboardBody = this.getDashboardBody(compiledMetrics, applicationInstanceId, workerHosts, maxParallelism,
      region, namespace, masterHost)
    this.getTemplate(dashboardName, dashboardBody)
  }

  /**
   * Get a CloudFormation template for a Dashboard.
   *
   * @param dashboardName Name of the dashboard.
   * @param dashboardBody Body of the dashboard definition.
   * @return Complete CloudFormation for the Dashboard.
   */
  private def getTemplate(dashboardName: String, dashboardBody: String): String = {
    "{\n  \"AWSTemplateFormatVersion\": \"2010-09-09\",\n  \"Resources\": {\n    \"Dashboard\": {\n      \"Type\": \"AWS::CloudWatch::Dashboard\",\n      \"Properties\": {\n        \"DashboardName\": \"DASHBOARDNAME\",\n        \"DashboardBody\": \"DASHBOARDBODY\"\n      }\n    }\n  }\n}"
      .replace(DashboardCompiler.DASHBOARD_NAME_PLACEHOLDER, dashboardName)
      .replace(DashboardCompiler.DASHBOARD_BODY_PLACEHOLDER, dashboardBody)
  }

  /**
   * Get the body of a dashboard CloudFormation template.
   *
   * @param compiledMetrics       List of metrics to include in the dashboard.
   * @param applicationInstanceId The ID of the instance that the dashboard is for.
   * @param workerHosts           List of IP addresses of the hosts that the job can be deployed to.
   * @param maxParallelism        The max-parallelism of the job.
   * @param region                The AWS region that the metrics are in.
   * @param namespace             The CloudWatch namespace of the metrics.
   * @param masterHost            The IP address of the master host that sends metrics to CloudWatch.
   * @return Body of CloudFormation Dashboard definition.
   */
  private def getDashboardBody(compiledMetrics: List[CompiledMetric],
                               applicationInstanceId: String,
                               workerHosts: List[String],
                               maxParallelism: Int,
                               region: String,
                               namespace: String,
                               masterHost: String): String = {
    val factory = new WidgetGeneratorFactory("Milan", region, masterHost, workerHosts,
      applicationInstanceId, 0, 300, false, false, "gauge")

    val dashboard = new DashboardJsonGenerator()
      .withWidget(new TextWidgetJsonGenerator(DashboardCompiler.AUTODASH_TEXT))

    val metricWidgets = compiledMetrics.map(compiledMetric => factory.getMetricWidget(compiledMetric))
    metricWidgets.foreach(widget => dashboard.withWidgets(widget))

    dashboard.toJson
  }
}

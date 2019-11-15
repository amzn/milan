package com.amazon.milan.flink.apps

import java.io.File
import java.nio.file.{AccessDeniedException, Files}

import com.amazon.milan.aws.metrics.DashboardCompiler
import com.amazon.milan.flink.compiler.FlinkCompiler
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder
import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest
import com.amazonaws.util.EC2MetadataUtils
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


object SerializedApplicationDashboardRunner {
  def main(args: Array[String]): Unit = {
    println("Starting application.  SerializedApplicationDashboardRunner")
    val logger = Logger(LoggerFactory.getLogger(this.getClass))

    val params = new CmdArgs()
    params.parse(args)

    logger.info(s"Loading application from resource '${params.applicationResourceName}'.")
    val instanceResourceStream = getClass.getResourceAsStream(params.applicationResourceName)

    logger.info("Compiling CloudFormation template for dashboard.")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val compilationResult = FlinkCompiler.defaultCompiler.compileFromInstanceJson(instanceResourceStream, env)

    val template = DashboardCompiler.compile(params.applicationInstanceId, compilationResult.compiledMetrics,
      getWorkerHostNames, params.maxParallelism, params.region, "Milan", getMasterHostName)

    logger.info("Writing CloudFormation template to file.")
    val template_path =
      this.writeTemplateToFile(
        params.applicationInstanceId,
        template,
        params.cloudFormationCacheFolder)

    logger.info(s"CloudFormation template written to file: $template_path")
  }

  /**
   * Get the IP addresses of the EMR cluster worker nodes.
   *
   * @return List of IP addresses in form of "ip-XX-X-X-XX".
   */
  private def getWorkerHostNames: List[String] = {
    val instanceId = EC2MetadataUtils.getInstanceId

    val ec2Client = AmazonEC2ClientBuilder.defaultClient()
    val describeInstancesResult = ec2Client.describeInstances(
      new DescribeInstancesRequest()
        .withInstanceIds(instanceId)
    )
    val tags = describeInstancesResult.getReservations.get(0).getInstances.get(0).getTags.asScala
    val clusterId = tags.filter(t => t.getKey == "aws:elasticmapreduce:job-flow-id").toList.head.getValue

    val emrClient = AmazonElasticMapReduceClientBuilder.defaultClient()
    val listInstancesResult = emrClient.listInstances(new ListInstancesRequest().withClusterId(clusterId))

    val hostNames = listInstancesResult.getInstances.asScala.map(i => i.getPrivateDnsName.split("\\.").head)
    hostNames.filter(_ != this.getMasterHostName).toList
  }

  /**
   * Get the IP address of the EMR cluster master node.
   *
   * @return IP address of master in form of "ip-XX-X-X-XX".
   */
  private def getMasterHostName: String = {
    EC2MetadataUtils.getLocalHostName.split("\\.").head
  }

  /**
   * Write CloudFormation template to file.
   *
   * @param applicationInstanceId  Instance ID of the application that the dashboard is for.
   * @param cloudFormationTemplate The CloudFormation template to be written to file.
   * @return Path of template file.
   */
  private def writeTemplateToFile(applicationInstanceId: String,
                                  cloudFormationTemplate: String,
                                  cloudFormationCacheFolder: String
                                 ): String = {
    val logger = Logger(LoggerFactory.getLogger(this.getClass))

    val cacheDirectory = new File(cloudFormationCacheFolder)
    if (!cacheDirectory.exists()) {
      cacheDirectory.mkdirs()
    }

    val templatePath = cacheDirectory.toPath.resolve(s"dashboard-$applicationInstanceId.json")
    logger.debug(s"Writing template to file: '${templatePath.toString}'.")
    try {
      Files.write(templatePath, cloudFormationTemplate.toCharArray.map(_.toByte))
    }
    catch {
      case ex: AccessDeniedException =>
        logger.error("Access denied. Could not write template to file.")
        throw ex
      case e: Exception => throw e
    }
    logger.debug(s"Template written to file: '${templatePath.toString}'.")
    templatePath.toString
  }

  private class CmdArgs extends ArgumentsBase {
    @NamedArgument(Name = "application-resource-name", ShortName = "app")
    var applicationResourceName: String = _

    @NamedArgument(Name = "application-instance-id", ShortName = "id")
    var applicationInstanceId: String = _

    @NamedArgument(Name = "region", ShortName = "r")
    var region: String = _

    @NamedArgument(Name = "cloudformation-cache-folder", ShortName = "cf")
    var cloudFormationCacheFolder: String = ""

    @NamedArgument(Name = "max-parallelism", ShortName = "p", Required = false, DefaultValue = "10")
    var maxParallelism: Int = _
  }

}

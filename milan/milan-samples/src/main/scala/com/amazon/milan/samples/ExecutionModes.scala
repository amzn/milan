package com.amazon.milan.samples

import java.nio.file.Paths
import java.time.{Duration, Instant}

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.sinks.KinesisDataSink
import com.amazon.milan.application.sources.{KinesisDataSource, ListDataSource}
import com.amazon.milan.application.{Application, ApplicationConfiguration}
import com.amazon.milan.flink.compiler.FlinkCompiler
import com.amazon.milan.flink.manage.FlinkApplicationManager
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.lang.{Stream, StreamGraph}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import software.amazon.awssdk.regions.Region

import scala.util.Random


/**
 * Demonstrates the different ways of executing a Milan application.
 */
object ExecutionModes {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty || args(0) == "inproc") {
      this.runInProcess()
    }
    else if (args(0) == "local") {
      this.runInLocalCluster()
    }
    else if (args(0) == "remote") {
      this.runOnRemoteCluster()
    }
  }

  /**
   * Executes the application in the current process using Flink's mini-cluster.
   */
  private def runInProcess(): Unit = {
    val graph = this.createApplication()
    val config = new ApplicationConfiguration()
    config.setSource("input", new ListDataSource(this.generateData()))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    env.execute()
  }

  /**
   * Executes the application using the Flink cluster on the local machine.
   * The application will be packaged using the current JAR.
   *
   * In order to work you must create a jar using maven-shade that includes all dependencies except for flink.
   * This can be done by running "mvn clean package".
   */
  private def runInLocalCluster(): Unit = {
    val graph = this.createApplication()
    val config = new ApplicationConfiguration()
    config.setSource("input", new ListDataSource(this.generateData()))

    val application = new Application(graph)

    val rootPath = Paths.get("")
    val manager = FlinkApplicationManager.createLocalFlinkApplicationManager(rootPath)

    // This packages the application and configuration into a JAR and executes it using the flink command on the local
    // machine.
    manager.startNewVersion(SemanticVersion.ZERO, application, config)
  }

  /**
   * Executes the application on a remote Flink cluster that has the Milan control plane running.
   * This example relies on Kinesis streams, so actual stream names will need to be provided before running it.
   *
   * In order to work you must create a jar using maven-shade that includes all dependencies except for flink.
   * This can be done by running "mvn clean package".
   */
  private def runOnRemoteCluster(): Unit = {
    val graph = this.createApplication()
    val config = new ApplicationConfiguration()

    val source = KinesisDataSource.createJson[DateValueRecord]("input-stream-name", "region")
    config.setSource("input", source)

    val sink = new KinesisDataSink[DateValueRecord]("output-stream-name", "region")
    config.addSink("output", sink)

    val application = new Application(graph)

    val manager = FlinkApplicationManager.createS3KinesisApplicationManager(
      Region.EU_WEST_1,
      "my-s3-bucket",
      "repository-root-folder",
      "controller-messages-stream",
      "controller-state-stream")

    manager.startNewVersion(SemanticVersion.ZERO, application, config)
  }

  private def generateData(): List[DateValueRecord] = {
    val now = Instant.now()
    val rand = new Random()

    List.tabulate(100)(_ => new DateValueRecord(now.plus(Duration.ofHours(rand.nextInt(240))), rand.nextDouble()))
  }

  private def createApplication(): StreamGraph = {
    // Define an external stream of DateValueRecord objects.
    val input = Stream.of[DateValueRecord].withId("input")

    // Define an output stream that provides the sum of values for each day.
    val dailySum =
      input
        .tumblingWindow(r => r.dateTime, Duration.ofDays(1), Duration.ZERO)
        .select((windowTime, record) => new DateValueRecord(windowTime, sum(record.value)))
        .withId("dailySum")

    new StreamGraph(dailySum)
  }
}

package com.amazon.milan.compiler.flink.testing

import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Duration

import com.amazon.milan.application.sinks.FileDataSink
import com.amazon.milan.application.{ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.RuntimeEvaluator
import com.amazon.milan.dataformats.{JsonDataInputFormat, JsonDataOutputFormat}
import com.amazon.milan.compiler.flink.generator.{FlinkGenerator, GeneratorConfig}
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.serialization.MilanObjectMapper
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits._


class ExecutionFailedException(command: String, stdOut: String, stdErr: String)
  extends Exception("Command execution failed.") {
}

class ApplicationExecutionResult(outputStreams: Map[Stream[_], List[_]]) {
  def getRecords[T](stream: Stream[T]): List[T] = {
    this.outputStreams(stream).map(_.asInstanceOf[T])
  }
}


object TestApplicationExecutor {
  /**
   * Executes a Milan application by generating a Flink program, compiling, and running it.
   * Data for the streams in the application can be captured and returned.
   *
   * @param instance              The application instance to execute.
   * @param maxRuntimeSeconds     The maximum allowed runtime for the generated application.
   * @param continuationPredicate A function that will be periodically called with the outputs collected up to that
   *                              point, which should return true if the application should be allowed to continue
   *                              running, and false if the application should be terminated.
   * @param outputStreams         Streams for which the records will be returned in the results.
   * @return An [[ApplicationExecutionResult]] object containing the records written to the output streams.
   */
  def executeApplication(instance: ApplicationInstance,
                         maxRuntimeSeconds: Int,
                         continuationPredicate: ApplicationExecutionResult => Boolean,
                         outputStreams: Stream[_]*): ApplicationExecutionResult = {
    this.executeApplication(
      instance.application.graph,
      instance.config,
      maxRuntimeSeconds,
      continuationPredicate,
      outputStreams: _*)
  }

  /**
   * Executes a Milan application by generating a Flink program, compiling, and running it.
   * Data for the streams in the application can be captured and returned.
   *
   * @param instance          The application instance to execute.
   * @param maxRuntimeSeconds The maximum allowed runtime for the generated application.
   * @param outputStreams     Streams for which the records will be returned in the results.
   * @return An [[ApplicationExecutionResult]] object containing the records written to the output streams.
   */
  def executeApplication(instance: ApplicationInstance,
                         maxRuntimeSeconds: Int,
                         outputStreams: Stream[_]*): ApplicationExecutionResult = {
    this.executeApplication(
      instance.application.graph,
      instance.config,
      maxRuntimeSeconds,
      outputStreams: _*)
  }

  /**
   * Executes a Milan application by generating a Flink program, compiling, and running it.
   * Data for the streams in the application can be captured and returned.
   *
   * @param graph             The Milan application graph.
   * @param config            The Milan application configuration.
   * @param maxRuntimeSeconds The maximum allowed runtime for the generated application.
   * @param outputStreams     Streams for which the records will be returned in the results.
   * @return An [[ApplicationExecutionResult]] object containing the records written to the output streams.
   */
  def executeApplication(graph: StreamGraph,
                         config: ApplicationConfiguration,
                         maxRuntimeSeconds: Int,
                         outputStreams: Stream[_]*): ApplicationExecutionResult = {
    val executor = new TestApplicationExecutor
    executor.executeApplication(graph, config, maxRuntimeSeconds, outputStreams: _*)
  }

  /**
   * Executes a Milan application by generating a Flink program, compiling, and running it.
   * Data for the streams in the application can be captured and returned.
   *
   * @param graph                 The Milan application graph.
   * @param config                The Milan application configuration.
   * @param maxRuntimeSeconds     The maximum allowed runtime for the generated application.
   * @param continuationPredicate A function that will be periodically called with the outputs collected up to that
   *                              point, which should return true if the application should be allowed to continue
   *                              running, and false if the application should be terminated.
   * @param outputStreams         Streams for which the records will be returned in the results.
   * @return An [[ApplicationExecutionResult]] object containing the records written to the output streams.
   */
  def executeApplication(graph: StreamGraph,
                         config: ApplicationConfiguration,
                         maxRuntimeSeconds: Int,
                         continuationPredicate: ApplicationExecutionResult => Boolean,
                         outputStreams: Stream[_]*): ApplicationExecutionResult = {
    val executor = new TestApplicationExecutor
    executor.executeApplication(graph, config, maxRuntimeSeconds, continuationPredicate, outputStreams: _*)
  }
}

/**
 * Methods for executing a Milan application locally for unit testing.
 */
class TestApplicationExecutor {
  // All of the runtime dependencies of the milan-flink-compiler package. This needs to be updated whenever the versions change in the pom.
  private val dependencies = "io.netty:netty-codec-http2:4.1.42.Final,org.apache.flink:flink-statebackend-rocksdb_2.12:1.9.1,org.scala-lang:scala-compiler:2.12.10,software.amazon.awssdk:emr:2.10.25,com.thoughtworks.paranamer:paranamer:2.8,io.netty:netty-common:4.1.42.Final,com.fasterxml.jackson.core:jackson-annotations:2.10.0,software.amazon.awssdk:utils:2.10.25,software.amazon.awssdk:auth:2.10.25,org.objenesis:objenesis:2.6,com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.10.0,com.esotericsoftware.minlog:minlog:1.2,com.fasterxml.jackson.core:jackson-core:2.10.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.amazonaws:jmespath-java:1.11.683,com.typesafe:ssl-config-core_2.12:0.3.7,software.amazon.awssdk:aws-core:2.10.25,com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.10.0,com.amazonaws:aws-java-sdk-kms:1.11.683,com.fasterxml.jackson.module:jackson-module-paranamer:2.10.0,org.scala-lang:scala-library:2.12.10,org.clapper:grizzled-slf4j_2.12:1.3.2,commons-cli:commons-cli:1.3.1,org.apache.logging.log4j:log4j-slf4j-impl:2.11.1,org.apache.flink:flink-shaded-netty:4.1.32.Final-7.0,io.netty:netty-resolver:4.1.42.Final,org.apache.logging.log4j:log4j-api:2.11.1,org.scala-lang.modules:scala-xml_2.12:1.0.6,com.typesafe.akka:akka-protobuf_2.12:2.5.21,com.google.code.findbugs:jsr305:1.3.9,org.apache.logging.log4j:log4j-core:2.11.1,org.apache.commons:commons-compress:1.18,software.amazon.awssdk:annotations:2.10.25,commons-lang:commons-lang:2.6,com.twitter:chill-java:0.7.6,com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.10.0,org.apache.flink:flink-streaming-scala_2.12:1.9.1,software.amazon.awssdk:regions:2.10.25,com.data-artisans:frocksdbjni:5.17.2-artisans-1.0,commons-codec:commons-codec:1.10,com.typesafe.scala-logging:scala-logging_2.12:3.9.2,org.apache.httpcomponents:httpclient:4.5.9,software.amazon.awssdk:apache-client:2.10.25,org.apache.httpcomponents:httpcore:4.4.11,software.amazon.awssdk:http-client-spi:2.10.25,org.scala-lang.modules:scala-parser-combinators_2.12:1.1.1,org.apache.flink:flink-annotations:1.9.1,org.apache.flink:flink-streaming-java_2.12:1.9.1,io.netty:netty-buffer:4.1.42.Final,com.google.guava:guava:18.0,io.netty:netty-transport-native-epoll:linux-x86_64,software.amazon.awssdk:aws-query-protocol:2.10.25,com.amazonaws:aws-java-sdk-core:1.11.683,org.apache.flink:flink-queryable-state-client-java:1.9.1,software.amazon.awssdk:s3:2.10.25,com.amazonaws:aws-java-sdk-s3:1.11.683,com.typesafe.akka:akka-actor_2.12:2.5.21,org.javassist:javassist:3.19.0-GA,org.scala-lang:scala-reflect:2.12.10,org.scala-lang.modules:scala-java8-compat_2.12:0.8.0,org.apache.flink:flink-core:1.9.1,org.apache.flink:flink-metrics-core:1.9.1,com.typesafe:config:1.3.3,io.dropwizard.metrics:metrics-core:3.1.5,org.apache.flink:flink-shaded-jackson:2.9.8-7.0,commons-io:commons-io:2.6,org.xerial.snappy:snappy-java:1.1.4,software.amazon.awssdk:aws-json-protocol:2.10.25,io.netty:netty-transport-native-unix-common:4.1.42.Final,com.typesafe.akka:akka-slf4j_2.12:2.5.21,software.amazon.awssdk:profiles:2.10.25,com.typesafe.netty:netty-reactive-streams:2.0.3,io.netty:netty-transport:4.1.42.Final,org.apache.flink:flink-runtime_2.12:1.9.1,org.apache.flink:flink-scala_2.12:1.9.1,org.reactivestreams:reactive-streams:1.0.2,com.github.scopt:scopt_2.12:3.5.0,com.fasterxml.jackson.core:jackson-databind:2.10.0,org.apache.flink:flink-hadoop-fs:1.9.1,org.apache.flink:flink-shaded-asm-6:6.2.1-7.0,org.apache.commons:commons-lang3:3.3.2,software.amazon.awssdk:protocol-core:2.10.25,com.typesafe.akka:akka-stream_2.12:2.5.21,io.netty:netty-codec:4.1.42.Final,org.apache.flink:flink-metrics-dropwizard:1.9.1,software.amazon.awssdk:kinesis:2.10.25,software.amazon.awssdk:ec2:2.10.25,com.twitter:chill_2.12:0.7.6,org.apache.commons:commons-math3:3.5,com.typesafe.netty:netty-reactive-streams-http:2.0.3,joda-time:joda-time:2.5,org.apache.flink:flink-shaded-guava:18.0-7.0,software.amazon.awssdk:netty-nio-client:2.10.25,software.amazon.eventstream:eventstream:1.0.1,com.esotericsoftware.kryo:kryo:2.24.0,org.apache.flink:flink-optimizer_2.12:1.9.1,commons-collections:commons-collections:3.2.2,software.amazon.awssdk:aws-cbor-protocol:2.10.25,io.netty:netty-codec-http:4.1.42.Final,org.slf4j:slf4j-api:1.7.25,commons-logging:commons-logging:1.1.3,software.amazon.ion:ion-java:1.0.2,org.apache.flink:force-shading:1.9.1,io.netty:netty-handler:4.1.42.Final,org.apache.flink:flink-connector-kinesis_2.11:1.7-SNAPSHOT,org.apache.flink:flink-java:1.9.1,software.amazon.awssdk:sdk-core:2.10.25,software.amazon.awssdk:aws-xml-protocol:2.10.25,org.apache.flink:flink-clients_2.12:1.9.1"

  // Test dependencies of the milan-flink package.
  private val testDependencies = "junit:junit:4.12"

  private val logger = Logger(LoggerFactory.getLogger(getClass))

  private val generator = new FlinkGenerator(GeneratorConfig())

  private var classPathLocations = getMilanClassPathEntries.toSet

  /**
   * Adds the location of a class to the classpath when compiling executing applications.
   *
   * @param cls The class whose location will be added to the classpath.
   */
  def addToClassPath(cls: Class[_]): Unit = {
    this.classPathLocations = this.classPathLocations ++ this.getClassPathEntries(cls)
  }

  /**
   * Executes a Milan application by generating a Flink program, compiling, and running it.
   * Data for the streams in the application can be captured and returned.
   *
   * @param instance              The application instance to execute.
   * @param maxRuntimeSeconds     The maximum allowed runtime for the generated application.
   * @param continuationPredicate A function that will be periodically called with the outputs collected up to that
   *                              point, which should return true if the application should be allowed to continue
   *                              running, and false if the application should be terminated.
   * @param outputStreams         Streams for which the records will be returned in the results.
   * @return An [[ApplicationExecutionResult]] object containing the records written to the output streams.
   */
  def executeApplication(instance: ApplicationInstance,
                         maxRuntimeSeconds: Int,
                         continuationPredicate: ApplicationExecutionResult => Boolean,
                         outputStreams: Stream[_]*): ApplicationExecutionResult = {
    this.executeApplication(
      instance.application.graph,
      instance.config,
      maxRuntimeSeconds,
      continuationPredicate,
      outputStreams: _*)
  }

  /**
   * Executes a Milan application by generating a Flink program, compiling, and running it.
   * Data for the streams in the application can be captured and returned.
   *
   * @param instance          The application instance to execute.
   * @param maxRuntimeSeconds The maximum allowed runtime for the generated application.
   * @param outputStreams     Streams for which the records will be returned in the results.
   * @return An [[ApplicationExecutionResult]] object containing the records written to the output streams.
   */
  def executeApplication(instance: ApplicationInstance,
                         maxRuntimeSeconds: Int,
                         outputStreams: Stream[_]*): ApplicationExecutionResult = {
    this.executeApplication(
      instance.application.graph,
      instance.config,
      maxRuntimeSeconds,
      outputStreams: _*)
  }

  /**
   * Executes a Milan application by generating a Flink program, compiling, and running it.
   * Data for the streams in the application can be captured and returned.
   *
   * @param graph                 The Milan application graph.
   * @param config                The Milan application configuration.
   * @param maxRuntimeSeconds     The maximum allowed runtime for the generated application.
   * @param continuationPredicate A function that will be periodically called with the outputs collected up to that
   *                              point, which should return true if the application should be allowed to continue
   *                              running, and false if the application should be terminated.
   * @param outputStreams         Streams for which the records will be returned in the results.
   * @return An [[ApplicationExecutionResult]] object containing the records written to the output streams.
   */
  def executeApplication(graph: StreamGraph,
                         config: ApplicationConfiguration,
                         maxRuntimeSeconds: Int,
                         continuationPredicate: ApplicationExecutionResult => Boolean,
                         outputStreams: Stream[_]*): ApplicationExecutionResult = {
    // Create a temporary folder to hold the generated code, compiled classes, and output files.
    val workingFolder = Files.createTempDirectory("milan_generated_")

    // Set up sinks for the output streams to write to files in the working folder.
    val outputFiles = this.addOutputSinks(workingFolder, config, outputStreams)

    // Generate the scala code for the application.
    val generatedCode = this.generator.generateScala(graph, config, "generated", "TestApplication")

    // Create the output directory for the compilation.
    val classesFolder = workingFolder.resolve("classes")
    Files.createDirectories(classesFolder)

    this.logger.info("Generated source:\n" + generatedCode)

    try {
      // Write the code to a file in the working folder.
      val codeFile = workingFolder.resolve("main.scala")
      this.logger.info(s"Writing code to temporary file '$codeFile'.")
      Files.write(codeFile, generatedCode.getBytes(StandardCharsets.UTF_8))

      val classPath = this.getClassPath

      // Compile the code by invoking scalac, if it fails with a non-zero exitcode an exception will be thrown.
      val compileCmd = s"scalac -d $classesFolder -classpath $classPath $codeFile"
      this.executeCommand(compileCmd, Duration.ofSeconds(20), () => true)

      // Execute the application by invoking java, if it fails with a non-zero exitcode an exception will be thrown.
      val mainClassName = s"generated.TestApplication"
      val runCmd = s"java -classpath $classesFolder/:$classPath $mainClassName"

      def collectResults(): ApplicationExecutionResult = {
        // Read the records from the output files.
        val outputs = this.collectOutputFileContents(outputFiles)
        new ApplicationExecutionResult(outputs)
      }

      // Periodically pass the current state of the results to the continuation predicate.
      this.executeCommand(runCmd, Duration.ofSeconds(maxRuntimeSeconds), () => continuationPredicate(collectResults()))

      collectResults()
    }
    catch {
      case ex: Exception =>
        this.logger.info("Error executing application. Source file:\n" + this.addLineNumbers(generatedCode))
        throw ex
    }
    finally {
      this.logger.info(s"Deleting temporary folder '$workingFolder'.")
      FileUtils.deleteDirectory(workingFolder.toFile)
    }
  }

  /**
   * Executes a Milan application by generating a Flink program, compiling, and running it.
   * Data for the streams in the application can be captured and returned.
   *
   * @param graph             The Milan application graph.
   * @param config            The Milan application configuration.
   * @param maxRuntimeSeconds The maximum allowed runtime for the generated application.
   * @param outputStreams     Streams for which the records will be returned in the results.
   * @return An [[ApplicationExecutionResult]] object containing the records written to the output streams.
   */
  def executeApplication(graph: StreamGraph,
                         config: ApplicationConfiguration,
                         maxRuntimeSeconds: Int,
                         outputStreams: Stream[_]*): ApplicationExecutionResult = {
    this.executeApplication(graph, config, maxRuntimeSeconds, _ => true, outputStreams: _*)
  }

  /**
   * Adds sinks for the output streams to an [[ApplicationConfiguration]].
   *
   * @param workingFolder The working folder where the application will be generated and executed.
   * @param config        The application configuration.
   * @param outputStreams The desired output streams.
   * @return List of tuples of [[Stream]] objects and the path to the output file where records from that stream will
   *         be written.
   */
  private def addOutputSinks(workingFolder: Path,
                             config: ApplicationConfiguration,
                             outputStreams: Seq[Stream[_]]): List[(Stream[_], Path)] = {
    // For each of the output streams, add a sink that writes that stream to a file in our working folder.
    val outputFiles = outputStreams.map(stream => (stream, workingFolder.resolve(stream.streamName + "_output.json"))).toList

    outputFiles.foreach {
      case (stream, outputFile) =>
        val recordType = stream.recordType.asInstanceOf[TypeDescriptor[Any]]
        val format = new JsonDataOutputFormat[Any]()(recordType)
        val sink = new FileDataSink[Any](outputFile.toString, format)(recordType)
        config.addSink(stream.asInstanceOf[Stream[Any]], sink)
    }

    outputFiles
  }

  /**
   * Collects the contents of a collection of files containing stream records in JSON format.
   *
   * @param outputFiles A list of tuples of stream definition and output file path.
   * @return A map of stream objects to the contents of the output files.
   *         The type of objects in the corresponding lists will be the same as the stream's record type parameter.
   */
  private def collectOutputFileContents(outputFiles: List[(Stream[_], Path)]): Map[Stream[_], List[_]] = {
    outputFiles
      .map {
        case (stream, outputFile) => stream -> this.collectOutputFileContents(outputFile, stream)
      }
      .toMap
  }

  /**
   * Reads the contents of a single output file.
   *
   * @param outputFile The path of the file to read.
   * @param stream     The stream object whose records are contained in the file.
   * @return A list of records read from the file. The type of the records will be the same as the stream's record
   *         type parameter.
   */
  private def collectOutputFileContents(outputFile: Path, stream: Stream[_]): List[_] = {
    if (!outputFile.toFile.exists()) {
      List.empty
    }
    else {
      val inputStream = new FileInputStream(outputFile.toFile)

      try {
        if (stream.recordType.isTuple) {
          // Reading tuples is different because they are written to the file as ArrayRecord objects but we want to
          // read them into scala tuples.
          this.readTuples(inputStream, stream.recordType)
        }
        else {
          val dataFormat = new JsonDataInputFormat[stream.RecordType]()(stream.recordType.asInstanceOf[TypeDescriptor[stream.RecordType]])
          dataFormat.readValues(inputStream).toList
        }
      }
      catch {
        case ex: Exception =>
          val fileContents = Files.readAllLines(outputFile).asScala.mkString("\n")
          this.logger.error("Error reading output file. File contents:\n" + fileContents)
          throw ex
      }
    }
  }

  /**
   * Returns tuples encoded as JSON objects from a file.
   *
   * @param inputStream The input stream containing the file contents.
   * @param recordType  A [[TypeDescriptor]] for the tuple type.
   * @return A list of tuple object read from the file.
   */
  private def readTuples(inputStream: InputStream, recordType: TypeDescriptor[_]): List[_] = {
    val createTuple = this.compileCreateTupleInstanceFunc(recordType)

    val lines = new BufferedReader(new InputStreamReader(inputStream)).lines().iterator().asScala
    val reader = MilanObjectMapper.readerFor(classOf[Array[Object]])

    lines
      .map(line => reader.readValue[Array[Any]](line))
      .map(createTuple)
      .toList
  }

  /**
   * Gets a method that creates tuple instances from arrays of objects.
   *
   * @param recordType A [[TypeDescriptor]] describing the destination tuple type.
   * @return A function that converts an array of objects into the desired tuple type.
   */
  private def compileCreateTupleInstanceFunc(recordType: TypeDescriptor[_]): Array[Any] => _ = {
    val eval = RuntimeEvaluator.instance

    val fieldTypeNames = recordType.genericArguments.map(_.fullName)

    // Create statements that get the tuple values from the list and cast them to the
    // expected type for the corresponding tuple element.
    val fieldValueGetters = recordType.genericArguments.zipWithIndex.map {
      case (ty, i) => this.getTypeConversionStatement(s"values($i)", ty)
    }

    val fieldValuesStatement = fieldValueGetters.mkString(", ")
    val tupleCreationStatement = s"Tuple${fieldTypeNames.length}($fieldValuesStatement)"

    this.logger.info(s"Compiling tuple creation function: $tupleCreationStatement")

    eval.createFunction[Array[Any], Product](
      "values",
      "Array[Any]",
      tupleCreationStatement
    )
  }

  /**
   * Gets a code snippet that converts the value of another snippet to the destination type.
   *
   * @param value           A code snippet representing the value to convert.
   * @param destinationType The type to convert to.
   * @return A code snippet that performs the conversion.
   */
  private def getTypeConversionStatement(value: String, destinationType: TypeDescriptor[_]): String = {
    if (destinationType == types.Instant) {
      // Instant objects are written as seconds.nanos and are interpreted as doubles by the json reader.
      s"java.time.Instant.ofEpochSecond($value.asInstanceOf[Double].toLong)"
    }
    else {
      s"$value.asInstanceOf[${destinationType.fullName}]"
    }
  }

  /**
   * Executes a command in a new process and waits for it to finish.
   *
   * @param command    The command to execute.
   * @param maxRuntime The maximum runtime to allow.
   */
  private def executeCommand(command: String, maxRuntime: Duration, continuationPredicate: () => Boolean): Unit = {
    val exec = new ProcessCommandExecutor(Some("/"))
    val futureResult = exec.execute(command, maxRuntime)

    // Wait until the process completes (or is terminated) or the continuation predicate tells us to stop.
    var processTerminated = false
    while (!futureResult.result.isCompleted && !processTerminated) {
      Thread.sleep(1000)

      // If the continuation predicate tells us to stop then kill the process immediately.
      // This should then cause the future to complete.
      if (!continuationPredicate()) {
        processTerminated = true
        futureResult.process.destroy()
        this.logger.info("Process terminated due to continuation predicate returning false.")
      }
    }

    val result = Await.result(futureResult.result, maxRuntime.toConcurrent)

    if (!result.isSuccess && !processTerminated) {
      throw new ExecutionFailedException(command, result.getFullStandardOutput, result.getFullErrorOutput)
    }
  }

  /**
   * Gets the full classpath as a colon-delimited string.
   */
  private def getClassPath: String = {
    val dependencyPaths = this.getDependencyPaths(this.dependencies.split(',') ++ this.testDependencies.split(','))
    (dependencyPaths ++ this.classPathLocations).mkString(":")
  }

  /**
   * Gets the locations that should be added to the classpath to support the specified class.
   */
  private def getClassPathEntries(cls: Class[_]): Seq[Path] = {
    val location = cls.getProtectionDomain.getCodeSource.getLocation.getFile

    if (location.toLowerCase().endsWith(".jar")) {
      // If it's a jar then add the jar.
      this.logger.info(s"Adding '$location' to classpath for Milan execution.")
      Seq(Paths.get(location))
    }
    else if (location.toLowerCase.endsWith("/test-classes/") || location.toLowerCase.endsWith("/classes/")) {
      // If it's a class file in a folder then add both the "classes" and "test-classes" folders to the classpath,
      // if they exist.
      val parent = Paths.get(location).getParent

      val paths =
        Seq(parent.resolve("test-classes"), parent.resolve("classes"))
          .filter(_.toFile.exists())

      paths.foreach(path => this.logger.info(s"Adding '$path' to classpath for Milan execution."))

      paths
    }
    else {
      throw new IllegalArgumentException(s"Couldn't resolve classpath for class '${cls.getName}'.")
    }
  }

  /**
   * Gets the classpath entries that are needed to load the Milan classes.
   */
  private def getMilanClassPathEntries: Seq[Path] = {
    val milanClasses = Seq(
      classOf[com.amazon.milan.compiler.flink.Compiler], // milan-flink-compiler
      classOf[com.amazon.milan.lang.StreamGraph], // milan-lang
      classOf[com.amazon.milan.typeutil.TypeProvider] // milan-typeutil
    )

    milanClasses.flatMap(this.getClassPathEntries)
  }

  /**
   * Gets the path of the local Maven repo.
   *
   * @return The root path of the Maven repo.
   */
  private def getLocalMvnRepo: Path = {
    val loggerJarFileName = this.logger.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
    val repoRoot = loggerJarFileName.split('/').takeWhile(_ != "repository").mkString("/")
    Paths.get(repoRoot).resolve("repository")
  }

  /**
   * Adds line numbers to a multiline string.
   *
   * @param text A string to add line numbers to.
   * @return The input string with the line number prepended to each line.
   */
  private def addLineNumbers(text: String): String = {
    text.linesIterator.zipWithIndex.map {
      case (line, i) => s"${i + 1}: $line"
    }.mkString("\n")
  }

  /**
   * Gets the full paths of a list of dependencies.
   *
   * @param dependencyInfos A list of dependencies. The format for dependency is "org:name:version".
   * @return A list of paths to the dependency jars.
   */
  private def getDependencyPaths(dependencyInfos: Seq[String]): Seq[Path] = {
    val mvnRoot = this.getLocalMvnRepo

    dependencyInfos.map(_.split(':')).map {
      case Array(pkg, name, version) =>
        val packagePath = pkg.replace('.', '/')
        mvnRoot.resolve(packagePath).resolve(name).resolve(version).resolve(s"$name-$version.jar")
    }
  }
}

package com.amazon.milan.manage

import java.io.FileOutputStream
import java.net.URI
import java.nio.file.{FileSystems, Files}
import java.util
import java.util.jar.{JarEntry, JarOutputStream, Manifest}

import com.amazon.milan.application.sinks.SingletonMemorySink
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.test.IntRecord
import com.amazon.milan.types.LineageRecord
import org.apache.commons.io.FileUtils
import org.junit.Assert._
import org.junit.Test


class TestApplicationPackager {
  @Test
  def test_ApplicationPackager_CreateJar_AddsSerializedApplicationToJarAndTheSerializedApplicationIsEquivalentToTheOriginal(): Unit = {
    val inputStream = Stream.of[IntRecord]

    val lineageSink = new SingletonMemorySink[LineageRecord]()
    val source = new ListDataSource(List(IntRecord(1)))

    val graph = new StreamGraph(inputStream)

    val app = new Application(graph)

    val config = new ApplicationConfiguration()
    config.setSource(inputStream, source)

    val sink = new SingletonMemorySink[IntRecord]
    config.addSink(inputStream, sink)
    config.addLineageSink(lineageSink)

    val rootFolder = Files.createTempDirectory("milan-test")

    try {
      // Create a simple jar file that will be the source.
      val sourceJarPath = rootFolder.resolve("source.jar")
      val sourceJarStream = new FileOutputStream(sourceJarPath.toString)
      val manifest = new Manifest()
      val outputStream = new JarOutputStream(sourceJarStream, manifest)

      val entry = new JarEntry("/existing_file.txt")
      outputStream.putNextEntry(entry)
      outputStream.write("Existing file contents.".getBytes("UTF-8"))
      outputStream.closeEntry()
      outputStream.close()

      val packageRepository = new LocalPackageRepository(rootFolder.resolve("packageRepo"))
      val packager = new JarApplicationPackager(sourceJarPath, packageRepository)

      // Add our application to the jar.
      val instance = new ApplicationInstance(app, config)
      val packageId = packager.packageApplication(instance)

      val destJarPath = packageRepository.getPackageFile(packageId)

      // Verify that it's there in the destination jar.
      val destJar = FileSystems.newFileSystem(URI.create("jar:file:" + destJarPath.toAbsolutePath.toString), new util.HashMap[String, String]())

      val applicationResourcePath = destJar.getPath("/application.json")
      val appResourceBytes = Files.readAllBytes(applicationResourcePath)
      val packagedInstance = ScalaObjectMapper.readValue[ApplicationInstance](appResourceBytes, classOf[ApplicationInstance])
      assertEquals(app, packagedInstance.application)
      assertEquals(config, packagedInstance.config)
    }
    finally {
      FileUtils.deleteDirectory(rootFolder.toFile)
    }
  }
}

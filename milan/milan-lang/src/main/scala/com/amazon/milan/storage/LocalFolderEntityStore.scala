package com.amazon.milan.storage

import java.nio.file.{Files, Path, StandardOpenOption}

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}


/**
 * [[EntityStore]] implementation that stores entities on the local file system.
 *
 * @param path The root path of the store.
 */
class LocalFolderEntityStore[T: ClassTag](path: Path, objectMapper: ObjectMapper) extends EntityStore[T] {
  private val entityClass = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override def getEntity(key: String): T = {
    val entityJson = Files.readAllBytes(this.getEntityPath(key))
    this.objectMapper.readValue[T](entityJson, this.entityClass)
  }

  override def putEntity(key: String, value: T): Unit = {
    val entityJson = this.objectMapper.writeValueAsBytes(value)
    Files.write(this.getEntityPath(key), entityJson, StandardOpenOption.CREATE_NEW)
  }

  override def entityExists(key: String): Boolean = {
    Files.exists(this.getEntityPath(key))
  }

  override def listEntityKeys(): TraversableOnce[String] = {
    Files
      .list(this.path)
      .iterator().asScala
      .filter(_.endsWith(".json"))
      .map(_.getFileName.toString.stripSuffix(".json"))
  }

  override def listEntities(): TraversableOnce[T] = {
    this.listEntityKeys().map(this.getEntity)
  }

  private def getEntityPath(key: String): Path =
    this.path.resolve(key + ".json")
}

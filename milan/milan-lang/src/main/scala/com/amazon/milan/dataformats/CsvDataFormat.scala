package com.amazon.milan.dataformats

import java.io.InputStream

import com.amazon.milan.serialization.{DataFormatConfiguration, DataFormatFlags, JavaTypeFactory}
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectReader}
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang.builder.HashCodeBuilder

import scala.collection.JavaConverters._


/**
 * A [[DataFormat]] for CSV-encoded objects.
 * This data format is only suitable for reading flat objects with fields that are basic types (i.e. numbers and strings).
 *
 * @param schema The schema of the CSV records. The column names in the schema must match properties of the object class.
 * @param config Configuration controlling data handling.
 * @tparam T The type of objects.
 */
@JsonDeserialize
@JsonSerialize
class CsvDataFormat[T: TypeDescriptor](val schema: Array[String],
                                       val config: DataFormatConfiguration)
  extends DataFormat[T] {

  @transient private lazy val hashCodeValue = HashCodeBuilder.reflectionHashCode(this)
  @transient private lazy val mapper: CsvMapper = this.createCsvMapper()
  @transient private lazy val reader: ObjectReader = this.createReader()
  @transient private lazy val recordJavaType = new JavaTypeFactory(this.mapper.getTypeFactory).makeJavaType(this.recordTypeDescriptor)

  private var recordTypeDescriptor: TypeDescriptor[T] = _

  // If created through deserialization the implicit TypeDescriptor parameter will be null, but otherwise
  // we want to do the unknown property check now.
  this.setGenericArguments(List(implicitly[TypeDescriptor[T]]))

  def this(schema: Array[String]) {
    this(schema, DataFormatConfiguration.default)
  }

  override def getGenericArguments: List[TypeDescriptor[_]] =
    List(implicitly[TypeDescriptor[T]])

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]

    if (this.recordTypeDescriptor != null) {
      this.verifyUnknownProperties()
    }
  }

  override def readValue(bytes: Array[Byte], offset: Int, length: Int): T = {
    this.reader.readValue[T](bytes, offset, length)
  }

  override def readValues(stream: InputStream): TraversableOnce[T] = {
    this.reader.readValues[T](stream).asScala
  }

  /**
   * Creates a [[CsvMapper]] using the data format configuration.
   */
  private def createCsvMapper(): CsvMapper = {
    val mapper = new CsvMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, this.config.isEnabled(DataFormatFlags.FailOnUnknownProperties))
    mapper
  }

  /**
   * Creates an [[ObjectReader]] for CSV rows using the format flags supplied in the constructor.
   */
  private def createReader(): ObjectReader = {
    val schemaBuilder = CsvSchema.builder()
    this.schema.foreach(schemaBuilder.addColumn)
    val csvSchema = schemaBuilder.build()
    mapper.readerFor(this.recordJavaType).`with`(csvSchema)
  }

  /**
   * If the FailOnUnknownProperties flag was used, verify that all schema columns match to a field or getter method
   * of the class.
   */
  private def verifyUnknownProperties(): Unit = {
    if (this.config.isEnabled(DataFormatFlags.FailOnUnknownProperties)) {
      val clazz = this.recordJavaType.getRawClass
      val fieldAndMethodNames = (clazz.getDeclaredFields.map(_.getName) ++ clazz.getDeclaredMethods.map(_.getName)).toSet

      this.schema.foreach(name => this.verifyPropertyExists(name, fieldAndMethodNames))
    }
  }

  /**
   * Verifies that a schema column has a matching field or getter method.
   *
   * @param schemaColumn        The name of the column.
   * @param fieldAndMethodNames The names of all of the fields and methods declared in the class.
   */
  private def verifyPropertyExists(schemaColumn: String, fieldAndMethodNames: Set[String]): Unit = {
    val getterName = "get" + schemaColumn.substring(0, 1).toUpperCase() + schemaColumn.substring(1)

    if (!fieldAndMethodNames.contains(schemaColumn) && !fieldAndMethodNames.contains(getterName)) {
      throw new PropertyNotFoundException(schemaColumn)
    }
  }

  override def hashCode(): Int = this.hashCodeValue

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: CsvDataFormat[T] =>
        this.config.equals(o.config) &&
          this.schema.sameElements(o.schema)

      case _ =>
        false
    }
  }
}

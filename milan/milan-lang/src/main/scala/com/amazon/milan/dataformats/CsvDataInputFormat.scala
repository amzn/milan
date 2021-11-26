package com.amazon.milan.dataformats

import com.amazon.milan.serialization.{DataFormatConfiguration, DataFormatFlags, JavaTypeFactory}
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectReader}
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.Logger
import org.apache.commons.lang.builder.HashCodeBuilder
import org.slf4j.LoggerFactory

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._


object CsvDataInputFormat {
  val DEFAULT_COLUMN_SEPARATOR: Char = ','
  val DEFAULT_NULL_IDENTIFIER: String = ""
}


/**
 * A [[DataInputFormat]] for CSV-encoded objects.
 * This data format is only suitable for reading flat objects with fields that are basic types (i.e. numbers and strings).
 *
 * @param schema          The schema of the CSV records. The column names in the schema must match properties of the object class.
 * @param skipHeader      Specifies that the first row read from the data source should be skipped because it contains
 *                        column headers.
 * @param columnSeparator The character used as a column separator.
 * @param nullIdentifier  A string that identifies null values.
 *                        Use an empty string for the default behavior, which is to not treat any values as special.
 * @param config          Configuration controlling data handling.
 * @tparam T The type of objects.
 */
@JsonDeserialize
@JsonSerialize
class CsvDataInputFormat[T: TypeDescriptor](val schema: Array[String],
                                            val skipHeader: Boolean,
                                            val columnSeparator: Char,
                                            val nullIdentifier: String,
                                            val config: DataFormatConfiguration)
  extends DataInputFormat[T] {

  @transient private lazy val hashCodeValue = HashCodeBuilder.reflectionHashCode(this)
  @transient private lazy val mapper: CsvMapper = this.createCsvMapper()
  @transient private lazy val reader: ObjectReader = this.createReader()
  @transient private lazy val recordJavaType = new JavaTypeFactory(this.mapper.getTypeFactory).makeJavaType(this.recordTypeDescriptor)
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))
  @transient private var readValueCount = 0
  @transient private var readValueErrorCount = 0

  private var recordTypeDescriptor: TypeDescriptor[T] = _

  // Calling setGenericArguments will do the check for unknown properties.
  // If created through deserialization the implicit TypeDescriptor parameter will be null and this code won't be called.
  this.setGenericArguments(List(implicitly[TypeDescriptor[T]]))

  def this(schema: Array[String], skipHeader: Boolean, columnSeparator: Char) {
    this(schema, skipHeader, columnSeparator, CsvDataInputFormat.DEFAULT_NULL_IDENTIFIER, DataFormatConfiguration.default)
  }

  def this(schema: Array[String], skipHeader: Boolean) {
    this(schema, skipHeader, CsvDataInputFormat.DEFAULT_COLUMN_SEPARATOR, CsvDataInputFormat.DEFAULT_NULL_IDENTIFIER, DataFormatConfiguration.default)
  }

  def this(schema: Array[String], config: DataFormatConfiguration) {
    this(schema, skipHeader = false, CsvDataInputFormat.DEFAULT_COLUMN_SEPARATOR, CsvDataInputFormat.DEFAULT_NULL_IDENTIFIER, config)
  }

  def this(schema: Array[String], columnSeparator: Char) {
    this(schema, skipHeader = false, columnSeparator, CsvDataInputFormat.DEFAULT_NULL_IDENTIFIER, DataFormatConfiguration.default)
  }

  def this(schema: Array[String]) {
    this(schema, skipHeader = false)
  }

  override def getGenericArguments: List[TypeDescriptor[_]] =
    List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]

    if (this.recordTypeDescriptor != null) {
      this.verifyUnknownProperties()
    }
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

  override def readValue(bytes: Array[Byte], offset: Int, length: Int): Option[T] = {
    val valueString = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes, offset, length)).toString
    if (this.skipHeader && valueString.startsWith(this.schema(0))) {
      None
    }
    else {
      this.readValueCount += 1
      try {
        Some(this.reader.readValue[T](valueString))
      }
      catch {
        case ex: Exception =>
          this.readValueErrorCount += 1
          val errorRate = this.readValueErrorCount.toFloat / this.readValueCount.toFloat
          this.logger.warn(s"Error '${ex.getMessage}' reading record, total error count = ${this.readValueErrorCount}, error rate = $errorRate.")
          None
      }
    }
  }

  override def readValues(stream: InputStream): TraversableOnce[T] = {
    this.reader.readValues[T](stream).asScala
  }

  override def hashCode(): Int = this.hashCodeValue

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: CsvDataInputFormat[T] =>
        this.config.equals(o.config) &&
          this.schema.sameElements(o.schema)

      case _ =>
        false
    }
  }

  /**
   * Creates a [[CsvMapper]] using the data format configuration.
   */
  private def createCsvMapper(): CsvMapper = {
    val mapper = new CsvMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.registerModule(new JavaTimeModule)
    mapper.registerModule(new InstantModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, this.config.isEnabled(DataFormatFlags.FailOnUnknownProperties))
    mapper.configure(CsvParser.Feature.IGNORE_TRAILING_UNMAPPABLE, !this.config.isEnabled(DataFormatFlags.FailOnUnknownProperties))
    mapper
  }

  /**
   * Creates an [[ObjectReader]] for CSV rows using the format flags supplied in the constructor.
   */
  private def createReader(): ObjectReader = {
    val schemaBuilder = CsvSchema.builder()
    this.schema.foreach(schemaBuilder.addColumn)
    val csvSchema = schemaBuilder.build().withColumnSeparator(this.columnSeparator)
    val csvSchemaWithNullValue = if (this.nullIdentifier.nonEmpty) csvSchema.withNullValue(this.nullIdentifier) else csvSchema
    mapper.readerFor(this.recordJavaType).`with`(csvSchemaWithNullValue)
  }
}

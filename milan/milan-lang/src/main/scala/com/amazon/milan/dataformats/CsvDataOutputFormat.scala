package com.amazon.milan.dataformats

import com.amazon.milan.HashUtil
import com.amazon.milan.serialization.JavaTypeFactory
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{ObjectReader, ObjectWriter}
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor


object CsvDataOutputFormat {

  /**
   * Configuration for the [[CsvDataOutputFormat]] output format.
   *
   * @param schema          Specifies the columns to write. These must match fields, properties, or getters on the type.
   * @param writeHeader     Specifies whether to write a header row before any output.
   *                        This should only be used if a single instance of this class will be used to write the output,
   *                        otherwise multiple header rows may be written.
   * @param dateTimeFormats A map of column names to datetime format strings.
   */
  class Configuration(val schema: Option[Array[String]],
                      val writeHeader: Boolean,
                      val dateTimeFormats: Map[String, String]) extends Serializable {
    def withSchema(schema: String*): Configuration =
      this.withSchema(schema.toArray)

    def withSchema(schema: Array[String]): Configuration =
      new Configuration(Some(schema), this.writeHeader, this.dateTimeFormats)

    def withWriteHeader(writeHeader: Boolean): Configuration =
      new Configuration(this.schema, writeHeader, this.dateTimeFormats)

    def withDateTimeFormat(columnName: String, dateTimeFormatString: String): Configuration =
      new Configuration(this.schema, this.writeHeader, this.dateTimeFormats + (columnName -> dateTimeFormatString))
  }

  object Configuration {
    val default: Configuration = new Configuration(
      schema = None,
      writeHeader = false,
      dateTimeFormats = Map())

    val defaultWithHeader: Configuration = this.default.withWriteHeader(true)
  }

}


/**
 * A [[DataOutputFormat]] that writes records as CSV rows.
 */
@JsonSerialize
@JsonDeserialize
class CsvDataOutputFormat[T: TypeDescriptor](val config: CsvDataOutputFormat.Configuration) extends DataOutputFormat[T] {
  @transient private lazy val hashCodeValue = HashUtil.combineHashCodes(this.recordTypeDescriptor.hashCode())
  @transient private lazy val mapper: CsvMapper = this.createCsvMapper()
  @transient private lazy val writer: ObjectWriter = this.createWriter()
  @transient private lazy val recordJavaType = new JavaTypeFactory(this.mapper.getTypeFactory).makeJavaType(this.recordTypeDescriptor)
  @transient private lazy val dateTimeFormatters = this.createDateTimeFormatters()

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]
  @transient private var headerWritten = false

  def this() {
    this(CsvDataOutputFormat.Configuration.default)
  }

  override def getGenericArguments: List[TypeDescriptor[_]] =
    List(implicitly[TypeDescriptor[T]])

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }

  override def writeValues(values: TraversableOnce[T], outputStream: OutputStream): Unit = {
    values.foreach(value => this.writeValue(value, outputStream))
  }

  override def writeValue(value: T, outputStream: OutputStream): Unit = {
    if (this.config.writeHeader && !headerWritten) {
      headerWritten = true
      this.writeHeader(outputStream)
    }

    if (this.recordTypeDescriptor.isNamedTuple) {
      this.writeNamedTupleLine(value, outputStream)
    }
    else {
      // ObjectWriter.writeValue closes the stream, so we have to write it to a temporary buffer.
      val buffer = new ByteArrayOutputStream()
      this.writer.writeValue(buffer, value)
      outputStream.write(buffer.toByteArray)
    }
  }

  private def writeNamedTupleLine(value: T, outputStream: OutputStream): Unit = {
    val fields =
      this.config.schema match {
        case Some(schema) => schema
        case None => this.recordTypeDescriptor.fields.map(_.name).toArray
      }

    val product = value.asInstanceOf[Product]

    val fieldValueStrings =
      fields.zipWithIndex.map { case (fieldName, i) => {
        val elem = product.productElement(i)
        val elemValue =
          this.dateTimeFormatters.get(fieldName) match {
            case Some(formatter) => formatter.format(elem.asInstanceOf[TemporalAccessor])
            case None => elem
          }

        elemValue match {
          case o if o == null => "null"
          case s: String => "\"" + s + "\""
          case o => o.toString
        }
      }
      }

    val line = fieldValueStrings.mkString(",") + "\n"
    val lineBytes = line.getBytes(StandardCharsets.UTF_8)
    outputStream.write(lineBytes)
  }

  private def writeHeader(outputStream: OutputStream): Unit = {
    val headerLine =
      this.config.schema match {
        case Some(schema) =>
          schema.map(f => "\"" + f + "\"").mkString(",")

        case None if this.recordTypeDescriptor.isNamedTuple =>
          this.recordTypeDescriptor.fields.map(f => "\"" + f.name + "\"").mkString(",")

        case None =>
          this.createSchema().getColumnDesc.stripPrefix("[").stripSuffix("]")
      }

    outputStream.write((headerLine + "\n").getBytes(StandardCharsets.UTF_8))
  }

  override def hashCode(): Int = this.hashCodeValue

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: CsvDataOutputFormat[T] =>
        this.recordTypeDescriptor.equals(o.recordTypeDescriptor)

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
    mapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true)
    mapper.registerModule(new ColumnDateTimeFormatsModule(this.dateTimeFormatters))

    mapper
  }

  /**
   * Creates an [[ObjectReader]] for CSV rows using the format flags supplied in the constructor.
   */
  private def createWriter(): ObjectWriter = {
    this.mapper.writerFor(this.recordJavaType).`with`(this.createSchema())
  }

  private def createSchema(): CsvSchema = {
    // If the caller provided an output schema then use that, otherwise use all the fields in the record type.
    val schemaBuilder = CsvSchema.builder()
    this.config.schema match {
      case Some(columns) =>
        val fieldTypes = this.recordTypeDescriptor.fields
          .map(field => field.name -> this.getColumnType(field.fieldType))
          .toMap

        columns.foreach(name =>
          fieldTypes.get(name) match {
            case Some(columnType) => schemaBuilder.addColumn(name, columnType)
            case None => schemaBuilder.addColumn(name)
          }
        )

      case None =>
        this.recordTypeDescriptor.fields
          .foreach(field => schemaBuilder.addColumn(field.name, this.getColumnType(field.fieldType)))
    }

    schemaBuilder.build()
  }

  private def getColumnType(ty: TypeDescriptor[_]): ColumnType = {
    if (ty.isNumeric) {
      ColumnType.NUMBER
    }
    else if (ty == types.Boolean) {
      ColumnType.BOOLEAN
    }
    else {
      ColumnType.STRING
    }
  }

  private def createDateTimeFormatters(): Map[String, DateTimeFormatter] = {
    this.config.dateTimeFormats.map {
      case (field, formatString) => field -> DateTimeFormatter.ofPattern(formatString)
    }
  }
}

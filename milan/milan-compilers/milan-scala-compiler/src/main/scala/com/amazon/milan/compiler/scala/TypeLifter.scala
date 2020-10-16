package com.amazon.milan.compiler.scala

import java.time.Duration

import com.amazon.milan.SemanticVersion
import com.amazon.milan.application.sources.FileDataSource
import com.amazon.milan.dataformats._
import com.amazon.milan.serialization.DataFormatConfiguration
import com.amazon.milan.typeutil.{BasicTypeDescriptor, CollectionTypeDescriptor, DataStreamTypeDescriptor, FieldDescriptor, GeneratedTypeDescriptor, GroupedStreamTypeDescriptor, JoinedStreamsTypeDescriptor, NumericTypeDescriptor, ObjectTypeDescriptor, TupleTypeDescriptor, TypeDescriptor}
import org.apache.commons.lang.StringEscapeUtils

import scala.collection.{AbstractSeq, immutable}
import scala.reflect.{ClassTag, classTag}


trait Raw {
  val value: String

  override def toString: String = this.value
}

object Raw {
  def unapply(arg: Raw): Option[String] = Some(arg.value)
}

case class RawList(l: List[String])

case class ClassName(value: String) extends Raw

case class ValName(value: String) extends Raw

case class MethodName(value: String) extends Raw

case class CodeBlock(value: String) extends Raw {
  def indentTail(level: Int): CodeBlock = {
    CodeBlock(value.indentTail(level))
  }
}

object CodeBlock {
  val EMPTY = CodeBlock("")
}


/**
 * Provides methods for lifting objects from runtime to compile-time.
 * Essentially, converting objects into the Scala code that constructs those objects.
 *
 * @param typeEmitter The [[TypeEmitter]] to use for emitting type names.
 */
class TypeLifter(val typeEmitter: TypeEmitter) {
  def this() {
    this(new DefaultTypeEmitter)
  }

  def withTypeEmitter(typeEmitter: TypeEmitter): TypeLifter = {
    new TypeLifter(typeEmitter)
  }

  implicit class Interpolator(sc: StringContext) {
    def q(subs: Any*): String = {
      val partsIterator = sc.parts.iterator
      val subsIterator = subs.iterator

      val sb = new StringBuilder(partsIterator.next())

      var unrollNextPart = false
      var unrollSeparator = ""

      while (subsIterator.hasNext) {
        val lifted =
          if (unrollNextPart) {
            liftSequence(subsIterator.next(), unrollSeparator)
          }
          else {
            lift(subsIterator.next())
          }
        sb.append(lifted)

        val nextPart = partsIterator.next()
        if (nextPart.endsWith("..")) {
          unrollNextPart = true
          unrollSeparator = ", "
          sb.append(nextPart.substring(0, nextPart.length - 2))
        }
        else if (nextPart.endsWith("//")) {
          unrollNextPart = true
          unrollSeparator = "\n"
          sb.append(nextPart.substring(0, nextPart.length - 2))
        }
        else {
          unrollNextPart = false
          sb.append(nextPart)
        }
      }

      sb.toString()
    }

    def qc(subs: Any*): CodeBlock = {
      val value = q(subs: _*).codeStrip
      CodeBlock(value)
    }

    def qn(subs: Any*): ClassName = {
      val value = q(subs: _*).codeStrip
      ClassName(value)
    }

    /**
     * Creates a [[CodeBlock]] containing a valid identifier composed from the input string and calling toString on the
     * objects.
     */
    def i(subs: Any*): CodeBlock = {
      val partsIterator = sc.parts.iterator
      val subsIterator = subs.iterator

      val sb = new StringBuilder(partsIterator.next())

      while (subsIterator.hasNext) {
        val nextSub = cleanIdentifier(subsIterator.next().toString)
        sb.append(nextSub)

        val nextPart = partsIterator.next()
        sb.append(nextPart)
      }

      CodeBlock(sb.toString())
    }
  }

  /**
   * Gets the fully-qualified name of a class, without generic type arguments.
   *
   * @tparam T The type whose name to get.
   * @return A [[ClassName]] containing the name of the type.
   */
  def nameOf[T: ClassTag]: ClassName =
    ClassName(this.toCanonicalName(classTag[T].runtimeClass.getName))

  /**
   * Converts a string into a [[CodeBlock]].
   * No processing is done in the input string to validate that it contains correct code.
   */
  def code(s: String): CodeBlock = CodeBlock(s)

  /**
   * Converts a list of strings into a [[RawList]].
   */
  def raw(l: List[String]): RawList = RawList(l)

  /**
   * Lifts a sequence of objects into a delimited string of lifted objects.
   * The lifted objects are written to the string as-is; there is not guarantee that they do not contain the delimiter
   * string.
   *
   * @param o         An object that implements [[AbstractSeq]].
   * @param delimiter The delimiter between lifted objects.
   * @return A string containing the lifted objects from the sequence, separated by the delimiter.
   */
  def liftSequence(o: Any, delimiter: String): String = {
    o match {
      case t: AbstractSeq[_] => t.map(lift).mkString(delimiter)
      case _ => throw new IllegalArgumentException(s"Object of type '${o.getClass.getTypeName}' is not a sequence.")
    }
  }

  /**
   * Lifts an object.
   *
   * @param o An object.
   * @return A string containing the compile-time representation of the object.
   */
  def lift(o: Any): String = {
    o match {
      case t: TypeDescriptor[_] => liftTypeDescriptor(t)
      case t: FieldDescriptor[_] => q"new ${nameOf[FieldDescriptor[Any]]}[${t.fieldType.toTerm}](${t.name}, ${t.fieldType})"
      case t: DataInputFormat[_] => liftDataInputFormat(t)
      case t: DataOutputFormat[_] => liftDataOutputFormat(t)
      case t: DataFormatConfiguration => q"new ${nameOf[DataFormatConfiguration]}(${t.flags})"
      case t: Array[_] => s"Array(${t.map(lift).mkString(", ")})"
      case t: List[_] => s"List(${t.map(lift).mkString(", ")})"
      case t: immutable.HashSet[_] => s"scala.collection.immutable.HashSet(${t.map(lift).mkString(", ")})"
      case t: Map[_, _] => s"Map(${t.map { case (k, v) => q"$k -> $v" }.mkString(", ")})"
      case Raw(s) => s
      case RawList(l) => l.mkString("List(", ", ", ")")
      case t: Char => "0x" + t.toHexString
      case t: String => if (t == null) "null" else "\"" + StringEscapeUtils.escapeJava(t) + "\""
      case t: Boolean => t.toString
      case t: Int => t.toString
      case t: Long => t.toString
      case t: FileDataSource.Configuration => q"${nameOf[FileDataSource.Configuration]}(${t.readMode})"
      case t if t == null => "null"
      case t if t.getClass.getTypeName == "scala.Enumeration$Val" => liftEnumeration(t)
      case t: SemanticVersion => q"new ${nameOf[SemanticVersion]}(${t.major}, ${t.minor}, ${t.patch}, ${t.preRelease}, ${t.buildMetadata})"
      case t: Duration => q"${nameOf[Duration]}.ofSeconds(${t.getSeconds}, ${t.getNano})"
      case t: Option[_] => this.liftOption(t)
      case t: CsvDataOutputFormat.Configuration => q"new ${nameOf[CsvDataOutputFormat.Configuration]}(${t.schema}, ${t.writeHeader}, ${t.dateTimeFormats})"
      case _ => throw new IllegalArgumentException(s"Can't lift object of type '${o.getClass.getTypeName}'.")
    }
  }

  /**
   * Gets the class name (not including type arguments) for a tuple with the specified number of elements.
   */
  def getTupleClassName(elementCount: Int): ClassName =
    ClassName(this.typeEmitter.getTupleClassName(elementCount))

  /**
   * Gets a [[CodeBlock]] that creates a tuple whose elements are provided as a collection of [[CodeBlock]] objects.
   *
   * @param elements A list of [[CodeBlock]] objects that return the tuple elements.
   * @return A [[CodeBlock]] that creates a tuple with the specified elements.
   */
  def getTupleCreationStatement(elements: List[CodeBlock]): CodeBlock = {
    qc"${this.getTupleClassName(elements.length)}(..$elements)"
  }

  /**
   * Gets the [[ClassName]] containing the fully-qualified name of a type, including generic arguments.
   */
  protected def classname(ty: TypeDescriptor[_]): ClassName = {
    ClassName(this.typeEmitter.getTypeFullName(ty))
  }

  protected def liftOption(optionValue: Option[_]): String = {
    optionValue match {
      case Some(value) => q"Some($value)"
      case None => "None"
    }
  }

  protected def liftEnumeration(value: Any): String = {
    val enumTypeName = value.getClass.getMethod("scala$Enumeration$Val$$$outer").invoke(value).getClass.getCanonicalName
    this.toCanonicalName(enumTypeName) + value.toString
  }

  protected def liftTypeDescriptor(value: TypeDescriptor[_]): String =
    value match {
      case t: NumericTypeDescriptor[_] => q"new ${nameOf[NumericTypeDescriptor[Any]]}[${classname(t)}](${t.typeName})"
      case t: TupleTypeDescriptor[_] => q"new ${nameOf[TupleTypeDescriptor[Any]]}[${classname(t)}](${t.typeName}, ${t.genericArguments}, ${t.fields})"
      case t: ObjectTypeDescriptor[_] => q"new ${nameOf[ObjectTypeDescriptor[Any]]}[${classname(t)}](${t.typeName}, ${t.genericArguments}, ${t.fields})"
      case t: GeneratedTypeDescriptor[_] => s"${nameOf[TypeDescriptor[Any]]}.of[${t.fullName}]"
      case t: CollectionTypeDescriptor[_] => q"new ${nameOf[CollectionTypeDescriptor[Any]]}[${classname(t)}](${t.typeName}, ${t.genericArguments})"
      case t: DataStreamTypeDescriptor => q"new ${nameOf[DataStreamTypeDescriptor]}(${t.recordType})"
      case t: JoinedStreamsTypeDescriptor => q"new ${nameOf[JoinedStreamsTypeDescriptor]}(${t.leftRecordType}, ${t.rightRecordType})"
      case t: GroupedStreamTypeDescriptor => q"new ${nameOf[GroupedStreamTypeDescriptor]}(${t.keyType}, ${t.recordType})"
      case t: BasicTypeDescriptor[_] => q"new ${nameOf[BasicTypeDescriptor[Any]]}[${classname(t)}](${t.typeName})"
    }

  protected def liftDataInputFormat(value: DataInputFormat[_]): String = {
    val recordType = value.getGenericArguments.head

    val defaultTypeLifter = this.withTypeEmitter(new DefaultTypeEmitter)
    val recordTypeName = ClassName(defaultTypeLifter.typeEmitter.getTypeFullName(recordType))
    val recordTypeStatement = CodeBlock(defaultTypeLifter.lift(recordType))

    value match {
      case t: JsonDataInputFormat[_] => q"new ${nameOf[JsonDataInputFormat[Any]]}[$recordTypeName](${t.config})($recordTypeStatement)"
      case t: CsvDataInputFormat[_] => q"new ${nameOf[CsvDataInputFormat[Any]]}[$recordTypeName](${t.schema}, ${t.skipHeader}, ${t.columnSeparator}, ${t.nullIdentifier}, ${t.config})($recordTypeStatement)"
    }
  }

  protected def liftDataOutputFormat(value: DataOutputFormat[_]): String = {
    val recordType = value.getGenericArguments.head

    val defaultTypeLifter = this.withTypeEmitter(new DefaultTypeEmitter)
    val recordTypeName = ClassName(defaultTypeLifter.typeEmitter.getTypeFullName(recordType))
    val recordTypeStatement = CodeBlock(defaultTypeLifter.lift(recordType))

    value match {
      case t: CsvDataOutputFormat[_] => q"new ${nameOf[CsvDataOutputFormat[Any]]}[$recordTypeName](${t.config})($recordTypeStatement)"
      case t: JsonDataOutputFormat[_] => q"new ${nameOf[JsonDataOutputFormat[Any]]}[$recordTypeName]()($recordTypeStatement)"
    }
  }

  protected def toCanonicalName(typeName: String): String =
    typeName.replace('$', '.')

  private def cleanIdentifier(id: String): String =
    id.replace('-', '_')
}

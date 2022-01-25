package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.flink.internal.{FlinkScalarFunctionGenerator, FlinkTypeEmitter}
import com.amazon.milan.compiler.flink.runtime.MilanApplicationBase
import com.amazon.milan.compiler.scala._
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.types.LineageRecord
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.OutputTag

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.mutable


case class ClassDef(classBody: String, imports: List[String])


/**
 * Collects the outputs from code generation.
 */
class GeneratorOutputs(val typeLifter: FlinkTypeLifter) {

  import typeLifter._

  val dataStreams = new mutable.HashMap[String, GeneratedDataStream]()
  val connectedStreams = new mutable.HashMap[String, GeneratedConnectedStreams]()
  val windowedStreams = new mutable.HashMap[String, GeneratedGroupedStream]()
  val scalaGenerator = new FlinkScalarFunctionGenerator(new FlinkTypeEmitter())

  /**
   * Map of type name to class name of RecordIdExtractor for that type.
   * Allows re-using record ID extractors for a given type.
   */
  val recordIdExtractorClasses = new mutable.HashMap[String, ClassName]()

  val streamEnvVal: ValName = ValName("env")
  val lineageOutputTag = qc"new ${nameOf[OutputTag[Any]]}[${nameOf[LineageRecord]}](${"lineage"}, org.apache.flink.api.scala.createTypeInformation[${nameOf[LineageRecord]}])"

  private var mainBlocks = List.empty[String]
  private var classDefs = List.empty[ClassDef]
  private var valNames = Set.empty[String]
  private var imports = Set.empty[String]
  private var hasCycles = false

  /**
   * Adds an import statement to the required imports for the generated code.
   *
   * @param importStatement An import statement, not including the import keyword.
   */
  def addImport(importStatement: String): Unit = {
    this.imports = this.imports + importStatement
  }

  /**
   * Appends a code block to the main function being generated.
   *
   * @param block A code block.
   */
  def appendMain(block: String): Unit = {
    this.mainBlocks = this.mainBlocks :+ block
  }

  /**
   * Adds a class definition to the outputs.
   *
   * @param classDef A string containing a class definition.
   */
  def addClassDef(classDef: String): Unit = {
    this.addClassDef(classDef, List())
  }

  /**
   * Adds a class definition to the outputs.
   *
   * @param classDef A string containing a class definition.
   * @param imports  A list of import statements (not including the import keyword) that the class definition requires.
   */
  def addClassDef(classDef: String, imports: List[String]): Unit = {
    this.classDefs = this.classDefs :+ ClassDef(classDef, imports)
  }

  /**
   * Generates the Scala code that has been added to the outputs collection so far.
   *
   * @param packageName The name of the package for the generated code.
   *                    If null or empty, no package statement is included at the beginning of the code.
   * @param className   The name of the object containing the main() method that executes the application.
   * @return
   */
  def generateScala(packageName: String, className: String): String = {
    val allImports = (this.classDefs.flatMap(_.imports).toSet ++ this.imports).toList.sorted.map(s => s"import $s")

    val packageLine = if (packageName == null || packageName.isEmpty) "" else s"package $packageName"

    s"""$packageLine
       |
       |${allImports.mkString("\n")}
       |
       |${this.classDefs.map(_.classBody).mkString("\n\n")}
       |
       |class $className extends ${nameOf[MilanApplicationBase]} {
       |  override def hasCycles: Boolean =
       |    ${this.hasCycles}
       |
       |  override def buildFlinkApplication(${this.streamEnvVal}: ${nameOf[StreamExecutionEnvironment]}): Unit = {
       |    ${this.mainBlocks.mkString("\n\n").indentTail(2)}
       |  }
       |}
       |
       |object $className {
       |  def main(args: Array[String]): Unit = {
       |    new $className().execute(args)
       |  }
       |}
       |""".codeStrip
  }

  def newValName(prefix: String): ValName = ValName(this.newName(prefix))

  def newClassName(prefix: String): ClassName = ClassName(this.newName(prefix))

  def getOperatorName(streamExpr: StreamExpression): String =
    streamExpr.nodeName

  def getGeneratedStreamVal(streamId: String): ValName =
    this.dataStreams(streamId).streamVal

  def getGeneratedStreamRecordType(streamId: String): TypeDescriptor[_] =
    this.dataStreams(streamId).recordType

  def newClassName(expr: StreamExpression, prefix: String): ClassName =
    ClassName(s"${prefix}_${toValidName(expr.nodeName)}")

  def newStreamValName(streamIdentifier: String): ValName =
    ValName(s"stream_${toValidName(streamIdentifier)}")

  def newStreamValName(expr: StreamExpression): ValName =
    this.newStreamValName(expr.nodeName)

  def newValName(expr: StreamExpression, prefix: String): ValName =
    ValName(s"${prefix}_${toValidName(expr.nodeName)}")

  def setHasCyclesTrue(): Unit =
    this.hasCycles = true

  @tailrec
  private def newName(prefix: String): String = {
    val name = toValidName(prefix + UUID.randomUUID().toString.substring(0, 8))
    if (this.valNames.contains(name)) {
      newName(prefix)
    }
    else {
      this.valNames = this.valNames + name
      name
    }
  }
}

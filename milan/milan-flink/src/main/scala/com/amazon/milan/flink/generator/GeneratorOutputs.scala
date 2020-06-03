package com.amazon.milan.flink.generator

import java.util.UUID

import com.amazon.milan.flink.internal.{FlinkTypeEmitter, TreeScalaConverter}
import com.amazon.milan.flink.runtime.MilanApplicationBase
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.types.LineageRecord
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.OutputTag

import scala.annotation.tailrec
import scala.collection.mutable


case class ClassDef(classBody: String, imports: List[String])


class GeneratorOutputs(val typeLifter: TypeLifter) {

  import typeLifter._

  val dataStreams = new mutable.HashMap[String, GeneratedDataStream]()
  val connectedStreams = new mutable.HashMap[String, GeneratedConnectedStreams]()
  val windowedStreams = new mutable.HashMap[String, GeneratedGroupedStream]()
  val scalaGenerator = new TreeScalaConverter(new FlinkTypeEmitter())

  /**
   * Map of type name to class name of RecordIdExtractor for that type.
   */
  val recordIdExtractorClasses = new mutable.HashMap[String, ClassName]()

  val streamEnvVal = ValName("env")
  val lineageOutputTag = qc"new ${nameOf[OutputTag[Any]]}[${nameOf[LineageRecord]}](${"lineage"}, org.apache.flink.api.scala.createTypeInformation[${nameOf[LineageRecord]}])"

  private var mainBlocks = List.empty[String]
  private var classDefs = List.empty[ClassDef]
  private var valNames = Set.empty[String]
  private var imports = Set.empty[String]
  private var hasCycles = false

  def addImport(importStatement: String): Unit = {
    this.imports = this.imports + importStatement
  }

  def appendMain(block: String): Unit = {
    this.mainBlocks = this.mainBlocks :+ block
  }

  def addClassDef(classDef: String): Unit = {
    this.addClassDef(classDef, List())
  }

  def addClassDef(classDef: String, imports: List[String]): Unit = {
    this.classDefs = this.classDefs :+ ClassDef(classDef, imports)
  }

  def generateScala(): String = {
    val allImports = (this.classDefs.flatMap(_.imports).toSet ++ this.imports).toList.sorted.map(s => s"import $s")

    s"""${allImports.mkString("\n")}
       |
       |${this.classDefs.map(_.classBody).mkString("\n\n")}
       |
       |class MilanApplication extends ${nameOf[MilanApplicationBase]} {
       |  override def hasCycles: Boolean =
       |    ${this.hasCycles}
       |
       |  override def buildFlinkApplication(${this.streamEnvVal}: ${nameOf[StreamExecutionEnvironment]}): Unit = {
       |    ${this.mainBlocks.mkString("\n\n").indentTail(2)}
       |  }
       |}
       |
       |object MilanApplicationRunner {
       |  def main(args: Array[String]): Unit = {
       |    new MilanApplication().execute(args)
       |  }
       |}
       |""".strip
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
    ClassName(s"${prefix}_${cleanName(expr.nodeName)}")

  def newStreamValName(streamIdentifier: String): ValName =
    ValName(s"stream_${cleanName(streamIdentifier)}")

  def newStreamValName(expr: StreamExpression): ValName =
    this.newStreamValName(expr.nodeName)

  def newValName(expr: StreamExpression, prefix: String): ValName =
    ValName(s"${prefix}_${cleanName(expr.nodeName)}")

  def cleanName(name: String): String =
    name.replace('-', '_')

  def setHasCyclesTrue(): Unit =
    this.hasCycles = true

  @tailrec
  private def newName(prefix: String): String = {
    val name = this.cleanName(prefix + UUID.randomUUID().toString.substring(0, 8))
    if (this.valNames.contains(name)) {
      newName(prefix)
    }
    else {
      this.valNames = this.valNames + name
      name
    }
  }
}

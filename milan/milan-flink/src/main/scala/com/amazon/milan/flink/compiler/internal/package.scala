package com.amazon.milan.flink.compiler

import java.time.Duration

import com.amazon.milan.flink.FlinkTypeNames
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.program.{GraphNodeExpression, InvalidProgramException, SingleInputGraphNodeExpression, StreamExpression}
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.streaming.api.windowing.time.Time

import scala.annotation.tailrec
import scala.language.implicitConversions


package object internal {

  implicit class TypeDescriptorExtensions(typeDesc: TypeDescriptor[_]) {
    def getTypeName: String = {
      if (this.typeDesc.fullName.startsWith(FlinkTypeNames.tuple)) {
        this.typeDesc.fullName
      }
      else if (this.typeDesc.isTuple) {
        // Convert canonical Tuple type names to Flink Java tuples.
        "org.apache.flink.api.java.tuple." + this.typeDesc.fullName
      }
      else {
        this.typeDesc.fullName
      }
    }

    def getRecordTypeName: String = {
      if (this.typeDesc.isStream) {
        this.typeDesc.asStream.recordType.getRecordTypeName
      }
      else if (this.typeDesc.isTuple) {
        ArrayRecord.typeName
      }
      else {
        this.getTypeName
      }
    }

    def getRecordType: TypeDescriptor[_] = {
      if (this.typeDesc.isStream) {
        this.typeDesc.asStream.recordType
      }
      else {
        this.typeDesc
      }
    }
  }


  implicit class DurationExtensions(duration: Duration) {
    def toFlinkTime: Time = Time.milliseconds(duration.toMillis)
  }


  implicit class GraphNodeExtensions(expr: GraphNodeExpression) {
    /**
     * Gets the record type of the nearest upstream input that is a [[StreamExpression]].
     */
    def getInputRecordType: TypeDescriptor[_] = this.findInputStream.recordType

    /**
     * Gets the name of the record type of the nearest upstream input that is a [[StreamExpression]].
     */
    def getInputRecordTypeName: String = this.getInputRecordType.getRecordTypeName

    /**
     * Gets the stream expression that represents the nearest upstream stream expression to this expression.
     *
     * For example, if this expression is MapRecord(UniqueBy(GroupBy(MapRecord(...)))) then this method returns
     * The inner MapRecord() expression because it is a StreamExpression while UniqueBy and GroupBy are not
     * StreamExpressions.
     */
    private def findInputStream: StreamExpression = {
      this.expr match {
        case SingleInputGraphNodeExpression(input) => findFirstStream(input)
        case _ => findFirstStream(this.expr)
      }
    }
  }


  implicit class StreamExpressionExtensions(stream: StreamExpression) {
    def getRecordTypeName: String = stream.tpe.getRecordTypeName
  }


  /**
   * Gets the stream expression that represents the nearest upstream stream expression to this expression,
   * including this expression in the search.
   *
   * For example, if this expression is MapRecord(UniqueBy(GroupBy(MapRecord(...)))) then this method returns
   * The inner MapRecord() expression because it is a StreamExpression while UniqueBy and GroupBy are not
   * StreamExpressions.
   */
  @tailrec
  def findFirstStream(expr: GraphNodeExpression): StreamExpression = {
    expr match {
      case s: StreamExpression => s
      case SingleInputGraphNodeExpression(input) => findFirstStream(input)
      case _ => throw new InvalidProgramException(s"Couldn't determine the input stream for expression: $expr")
    }
  }
}

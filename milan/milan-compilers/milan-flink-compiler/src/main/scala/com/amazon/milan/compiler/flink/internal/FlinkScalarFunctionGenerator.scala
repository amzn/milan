package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.compiler.scala.{CodeBlock, DefaultTypeEmitter, ScalarFunctionGenerator, TypeEmitter}
import com.amazon.milan.compiler.flink.generator.FlinkGeneratorException
import com.amazon.milan.compiler.flink.typeutil._
import com.amazon.milan.program.ValueDef
import com.amazon.milan.types._
import com.amazon.milan.typeutil.{TypeDescriptor, types}

import scala.language.existentials


object FlinkScalarFunctionGenerator {
  val default = new FlinkScalarFunctionGenerator(new DefaultTypeEmitter)
}


case class FunctionParts(arguments: CodeBlock, returnType: CodeBlock, body: CodeBlock)


class FlinkScalarFunctionGenerator(typeEmitter: TypeEmitter) extends ScalarFunctionGenerator(typeEmitter, ContextualTreeTransformer) {

  /**
   * Context that accepts a single term name which refers to a named argument that is a tuple stream type.
   */
  private class ArrayArgumentConversionContext(argName: String, argType: TypeDescriptor[_]) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      if (name != this.argName) {
        throw new FlinkGeneratorException(s"Term '$name' does not match the function argument '${this.argName}'.")
      }

      (name, new ArrayFieldConversionContext(argType))
    }
  }


  /**
   * Context where term names refer to fields in a tuple stream.
   */
  private class ArrayFieldConversionContext(tupleType: TypeDescriptor[_]) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      if (name == RecordIdFieldName) {
        // RecordId is a special field for tuple streams, because it's a property of the ArrayRecord class rather than
        // being present in the fields array itself.
        (s".$name", createContextForType(types.String))
      }
      else {
        val fieldIndex = this.tupleType.fields.takeWhile(_.name != name).length

        if (fieldIndex >= this.tupleType.fields.length) {
          throw new FlinkGeneratorException(s"Field '$name' not found.")
        }

        val fieldType = this.tupleType.fields(fieldIndex).fieldType
        (s"($fieldIndex).asInstanceOf[${typeEmitter.getTypeFullName(fieldType)}]", createContextForType(fieldType))
      }
    }
  }

  override protected def createContextForArgument(valueDef: ValueDef): ConversionContext = {
    // If the record type is a tuple with named fields then this is a tuple stream whose records are stored as
    // ArrayRecord objects.
    if (valueDef.tpe.isTupleRecord) {
      new ArrayArgumentConversionContext(valueDef.name, valueDef.tpe)
    }
    else {
      super.createContextForArgument(valueDef)
    }
  }

  override protected def createContextForType(contextType: TypeDescriptor[_]): ConversionContext = {
    // If the context type is a tuple with named fields then term names must be mapped to indices in the ArrayRecord
    // objects.
    if (contextType.isTupleRecord) {
      new ArrayFieldConversionContext(contextType)
    }
    else {
      super.createContextForType(contextType)
    }
  }
}

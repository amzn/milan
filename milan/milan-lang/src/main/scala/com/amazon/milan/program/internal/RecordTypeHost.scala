package com.amazon.milan.program.internal

import com.amazon.milan.program.{FunctionDef, NamedFields}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, FieldDescriptor, GroupedStreamTypeDescriptor, StreamTypeDescriptor, TupleTypeDescriptor, TypeDescriptor, TypeDescriptorMacroHost}

import scala.reflect.macros.whitebox

trait RecordTypeHost extends TypeDescriptorMacroHost {
  val c: whitebox.Context

  import c.universe._

  def getStreamTypeExpr[T: c.WeakTypeTag](producingFunction: c.Expr[FunctionDef]): c.Expr[StreamTypeDescriptor] = {
    val recordType = getTypeDescriptor[T]
    val tree = q"new ${typeOf[DataStreamTypeDescriptor]}(com.amazon.milan.program.internal.RecordTypeUtil.addFieldNames($recordType, $producingFunction))"
    c.Expr[StreamTypeDescriptor](tree)
  }

  def getGroupedStreamTypeExpr[T: c.WeakTypeTag, TKey: c.WeakTypeTag](producingFunction: c.Expr[FunctionDef]): c.Expr[GroupedStreamTypeDescriptor] = {
    val recordType = getTypeDescriptor[T]
    val keyType = getTypeDescriptor[TKey]
    val tree = q"new ${typeOf[GroupedStreamTypeDescriptor]}($keyType, com.amazon.milan.program.internal.RecordTypeUtil.addFieldNames($recordType, $producingFunction))"
    c.Expr[GroupedStreamTypeDescriptor](tree)
  }

  def getRecordTypeExpr[T: c.WeakTypeTag](producingFunction: c.Expr[FunctionDef]): c.Expr[TypeDescriptor[T]] = {
    this.getRecordTypeExpr[T](getTypeDescriptor[T], producingFunction)
  }

  def getRecordTypeExpr[T](recordType: TypeDescriptor[T],
                           producingFunction: c.Expr[FunctionDef]): c.Expr[TypeDescriptor[T]] = {
    val tree = q"com.amazon.milan.program.internal.RecordTypeUtil.addFieldNames($recordType, $producingFunction)"
    c.Expr[TypeDescriptor[T]](tree)
  }
}


object RecordTypeUtil {
  def addFieldNames[T](recordType: TypeDescriptor[T], producingFunction: FunctionDef): TypeDescriptor[T] = {
    if (recordType.fields.nonEmpty || recordType.genericArguments.isEmpty) {
      recordType
    }
    else {
      producingFunction.body match {
        case NamedFields(fields) if fields.length == recordType.genericArguments.length =>
          val fieldDescriptors = fields.zip(recordType.genericArguments).map {
            case (field, ty) => FieldDescriptor(field.fieldName, ty)
          }

          new TupleTypeDescriptor[T](fieldDescriptors)

        case _ =>
          recordType
      }
    }
  }
}

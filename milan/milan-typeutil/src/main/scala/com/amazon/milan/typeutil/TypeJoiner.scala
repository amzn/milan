package com.amazon.milan.typeutil

import java.nio.file.StandardOpenOption

import scala.collection.JavaConverters._
import scala.reflect.macros.whitebox


trait TypeJoiner[TLeft, TRight] {
  type OutputType <: Product

  def getOutputType(leftType: TypeDescriptor[TLeft],
                    rightType: TypeDescriptor[TRight]): TypeDescriptor[OutputType]
}


object TypeJoiner {
  def getOutputType[TLeft, TRight, TOut](leftType: TypeDescriptor[TLeft],
                                         rightType: TypeDescriptor[TRight]): TypeDescriptor[TOut] = {
    val outputTypeName = this.getTupleTypeName(leftType, rightType)

    if (leftType.isTuple && rightType.isTuple) {
      // Both are tuple streams so merge the lists of fields.
      val combinedFields = this.combineFieldLists(leftType.fields, rightType.fields)
      val combinedGenericArgs = leftType.genericArguments ++ rightType.genericArguments
      new TupleTypeDescriptor[TOut](outputTypeName, combinedGenericArgs, combinedFields)
    }
    else if (leftType.isTuple) {
      // The right stream is an object stream, so it's records need to be a single field in the output.
      val rightField = this.createFieldWithUniqueName(rightType, "right", leftType.fields)
      val combinedFields = leftType.fields ++ List(rightField)
      val combinedGenericArgs = leftType.genericArguments ++ List(rightType)
      new TupleTypeDescriptor[TOut](outputTypeName, combinedGenericArgs, combinedFields)
    }
    else if (rightType.isTuple) {
      // The left stream is an object stream, so it's records need to be a single field in the output.
      val leftField = this.createFieldWithUniqueName(leftType, "left", rightType.fields)
      val combinedFields = List(leftField) ++ rightType.fields
      val combinedGenericArgs = List(leftType) ++ rightType.genericArguments
      new TupleTypeDescriptor[TOut](outputTypeName, combinedGenericArgs, combinedFields)
    }
    else {
      // We have two object streams, combine them into a tuple stream with two fields.
      val combinedFields = List(FieldDescriptor[TLeft]("left", leftType), FieldDescriptor[TRight]("right", rightType))
      val combinedGenericArgs = List(leftType, rightType)
      new TupleTypeDescriptor[TOut](outputTypeName, combinedGenericArgs, combinedFields)
    }
  }

  private def combineFieldLists(leftFields: List[FieldDescriptor[_]],
                                rightFields: List[FieldDescriptor[_]]): List[FieldDescriptor[_]] = {
    val leftFieldNames = leftFields.map(_.name).toSet
    val uniqueNameRightFields = this.ensureUniqueFieldNames(rightFields, leftFieldNames, "right_")
    leftFields ++ uniqueNameRightFields
  }

  private def ensureUniqueFieldNames(fields: List[FieldDescriptor[_]],
                                     existingNames: Set[String],
                                     duplicatePrefix: String): List[FieldDescriptor[_]] = {
    // scanLeft, adding names we find to the set of existing names and passing it along.
    // Output the current set of names and the current field (with a unique name) at every step.
    fields.scanLeft((existingNames, None: Option[FieldDescriptor[_]]))((t, field) => t match {
      case (names, _) =>
        val uniqueName = if (names.contains(field.name)) duplicatePrefix + field.name else field.name
        (names + uniqueName, Some(field.rename(uniqueName)))
    })
      .drop(1)
      .map {
        case (_, Some(field)) => field
        case _ => throw new Exception("You shouldn't be here.")
      }
  }

  private def createFieldWithUniqueName[T](fieldType: TypeDescriptor[T],
                                           name: String,
                                           existingFields: List[FieldDescriptor[_]]): FieldDescriptor[T] = {
    val existingNames = existingFields.map(_.name).toSet
    val uniqueName = this.getUniqueFieldName(name, existingNames)
    FieldDescriptor(uniqueName, fieldType)
  }

  private def getUniqueFieldName(name: String, existingNames: Set[String]): String = {
    if (!existingNames.contains(name)) {
      name
    }
    else {
      Iterator.from(1).map(i => s"$name$i").dropWhile(existingNames.contains).next()
    }
  }

  private def getTupleTypeName(left: TypeDescriptor[_], right: TypeDescriptor[_]): String = {
    val elementCount = (left.isTuple, right.isTuple) match {
      case (true, true) => left.genericArguments.length + right.genericArguments.length
      case (true, false) => left.genericArguments.length + 1
      case (false, true) => right.genericArguments.length + 1
      case (false, false) => 2
    }
    s"Tuple$elementCount"
  }
}


class TypeJoinerMacros(val c: whitebox.Context) extends TypeDescriptorMacroHost {

  import c.universe._

  def createTypeJoiner[TLeft: c.WeakTypeTag, TRight: c.WeakTypeTag]: c.Expr[TypeJoiner[TLeft, TRight]] = {
    val leftType = c.weakTypeOf[TLeft]
    val rightType = c.weakTypeOf[TRight]

    val outputType = this.getCombinedTupleType(leftType, rightType)

    val tree =
      q"""
         new ${weakTypeOf[TypeJoiner[TLeft, TRight]]} {
           type OutputType = $outputType

           override def getOutputType(leftType: TypeDescriptor[$leftType], rightType: TypeDescriptor[$rightType]): TypeDescriptor[this.OutputType] = {
             com.amazon.milan.typeutil.TypeJoiner.getOutputType[$leftType, $rightType, $outputType](leftType, rightType)
           }
         }
       """
    c.Expr[TypeJoiner[TLeft, TRight]](tree)
  }

  private val tupleTypes = Array(
    symbolOf[Tuple1[_]],
    symbolOf[(_, _)],
    symbolOf[(_, _, _)],
    symbolOf[(_, _, _, _)],
    symbolOf[(_, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
    symbolOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)]
  )

  private def getCombinedTupleType(leftType: Type, rightType: Type): Type = {
    val combinedTypes =
      if (isTuple(leftType) && isTuple(rightType)) {
        leftType.typeArgs ++ rightType.typeArgs
      }
      else if (isTuple(leftType)) {
        leftType.typeArgs ++ List(rightType)
      }
      else if (isTuple(rightType)) {
        List(leftType) ++ rightType.typeArgs
      }
      else {
        List(leftType, rightType)
      }

    if (combinedTypes.length > 22) {
      c.error(c.enclosingPosition, s"Combined type has ${combinedTypes.length} fields, but the maximum allowed number of fields is 22. Consider creating a record class rather than using named fields.")
    }

    val typeSymbol = tupleTypes(combinedTypes.length - 1)
    appliedType(typeSymbol, combinedTypes)
  }
}

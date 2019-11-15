package com.amazon.milan.lang.internal

import com.amazon.milan.Id
import com.amazon.milan.lang.{FieldStatement, ObjectStream, TupleStream}
import com.amazon.milan.program._
import com.amazon.milan.program.internal.FilteredStreamHost
import com.amazon.milan.types.{Record, RecordIdFieldName}
import com.amazon.milan.typeutil.{FieldDescriptor, StreamTypeDescriptor, TupleTypeDescriptor, TypeDescriptor, TypeJoiner}

import scala.reflect.macros.whitebox


/**
 * Macro bundle for operations on [[TupleStream]] objects.
 *
 * @param c The macro context.
 */
class TupleStreamMacros(val c: whitebox.Context) extends FilteredStreamHost with LangTypeNamesHost with FieldStatementHost {

  import c.universe._

  /**
   * Creates a [[TupleStream]] that is the result of applying a filter operation.
   *
   * @param predicate The filter predicate expression.
   * @tparam T The type of the stream.
   * @return An [[TupleStream]] representing the filtered stream.
   */
  def where[T <: Product : c.WeakTypeTag](predicate: c.Expr[T => Boolean]): c.Expr[TupleStream[T]] = {
    val nodeTree = this.createdFilteredStream[T](predicate)

    val inputVal = TermName(c.freshName())
    val streamType = c.weakTypeOf[T]

    val tree =
      q"""
          val $inputVal = ${c.prefix}
          new ${tupleStreamTypeName(streamType)}($nodeTree, $inputVal.fields)
       """
    c.Expr[TupleStream[T]](tree)
  }

  /**
   * Projects the fields from this stream onto the fields of the specified object record type.
   *
   * @tparam T       The type of the input stream.
   * @tparam TTarget The type of record objects to project the stream fields onto.
   * @return An [[ObjectStream]] of the target record type.
   */
  def projectOnto[T <: Product : c.WeakTypeTag, TTarget <: Record : c.WeakTypeTag]: c.Expr[ObjectStream[TTarget]] = {
    val targetTypeInfo = this.createTypeInfo[TTarget]

    // The target type needs a constructor that takes the recordId and the stream fields.
    // For now let's require that to be the primary constructor.
    // Yes, this actually gets the arguments for the primary constructor.
    val constructorParamNames = c.weakTypeOf[TTarget].typeSymbol.asClass.primaryConstructor.asMethod.paramLists.head.map(_.name.toString)

    val inputType = c.weakTypeOf[T]
    val targetType = c.weakTypeOf[TTarget]

    val tree = q"com.amazon.milan.lang.internal.TupleStreamUtil.projectTupleStream[$inputType, $targetType](${c.prefix}, ${targetTypeInfo.toTypeDescriptor}, $constructorParamNames)"

    c.Expr[ObjectStream[TTarget]](tree)
  }

  /**
   * Adds a field to a tuple stream.
   *
   * @param f      A [[FieldStatement]] describing the field to add, as computed from the existing records.
   * @param joiner A [[TypeJoiner]] that produces output type information.
   * @tparam T  The type of the input stream.
   * @tparam TF The type of the field being added.
   * @return A [[TupleStream]] that contains the input fields and the additional field.
   */
  def addField[T <: Product : c.WeakTypeTag, TF: c.WeakTypeTag](f: c.Expr[FieldStatement[T, TF]])
                                                               (joiner: c.Expr[TypeJoiner[T, Tuple1[TF]]]): c.universe.Tree = {
    val expr = getFieldDefinition[T, TF](f)
    this.addFields[T, Tuple1[TF]](List((expr, c.weakTypeOf[TF])))(joiner)
  }

  def addFields2[T <: Product : c.WeakTypeTag, TF1: c.WeakTypeTag, TF2: c.WeakTypeTag](f1: c.Expr[FieldStatement[T, TF1]],
                                                                                       f2: c.Expr[FieldStatement[T, TF2]])
                                                                                      (joiner: c.Expr[TypeJoiner[T, (TF1, TF2)]]): c.universe.Tree = {
    val expr1 = getFieldDefinition(f1)
    val expr2 = getFieldDefinition(f2)
    this.addFields[T, (TF1, TF2)](List((expr1, c.weakTypeOf[TF1]), (expr2, c.weakTypeOf[TF2])))(joiner)
  }

  def addFields3[T <: Product : c.WeakTypeTag, TF1: c.WeakTypeTag, TF2: c.WeakTypeTag, TF3: c.WeakTypeTag](f1: c.Expr[FieldStatement[T, TF1]],
                                                                                                           f2: c.Expr[FieldStatement[T, TF2]],
                                                                                                           f3: c.Expr[FieldStatement[T, TF3]])
                                                                                                          (joiner: c.Expr[TypeJoiner[T, (TF1, TF2, TF3)]]): c.universe.Tree = {
    val expr1 = getFieldDefinition(f1)
    val expr2 = getFieldDefinition(f2)
    val expr3 = getFieldDefinition(f3)
    this.addFields[T, (TF1, TF2, TF3)](List((expr1, c.weakTypeOf[TF1]), (expr2, c.weakTypeOf[TF2]), (expr3, c.weakTypeOf[TF3])))(joiner)
  }

  private def addFields[T <: Product : c.WeakTypeTag, TAdd <: Product : c.WeakTypeTag](fields: List[(c.Expr[FieldDefinition], c.Type)])
                                                                                      (joiner: c.Expr[TypeJoiner[T, TAdd]]): c.universe.Tree = {
    val leftType = c.weakTypeOf[T]
    val rightType = c.weakTypeOf[TAdd]

    val newFieldDefs = fields.map { case (f, _) => f }

    val newTupleTypeInfo = createTypeInfo[TAdd]
    val newFieldTypes = newTupleTypeInfo.genericArguments.map(_.toTypeDescriptor)

    q"com.amazon.milan.lang.internal.TupleStreamUtil.addFieldsToTupleStream[$leftType, $rightType](${c.prefix}, $newFieldDefs, $newFieldTypes, $joiner)"
  }
}


object TupleStreamUtil {
  /**
   * Gets the [[ObjectStream]] that results from projecting the fields of a [[TupleStream]] onto a record type.
   *
   * @param inputStream           The [[TupleStream]] whose fields will be projected onto the record type.
   * @param targetType            A [[TypeDescriptor]] for the target record type.
   * @param constructorParamNames The names of the constructor parameters that the tuple fields will be mapped to.
   * @tparam T       The type of the tuple stream records.
   * @tparam TTarget The type of the projection target.
   * @return An [[ObjectStream]] representing the result of the projection operation.
   */
  def projectTupleStream[T <: Product, TTarget <: Record](inputStream: TupleStream[T],
                                                          targetType: TypeDescriptor[TTarget],
                                                          constructorParamNames: List[String]): ObjectStream[TTarget] = {
    val fieldNames = inputStream.fields.map(_.name)
    val projectFunctionDef = this.createProjectionFunction(fieldNames, targetType, constructorParamNames)
    val outputNodeId = Id.newId()
    val streamType = new StreamTypeDescriptor(targetType)
    val streamExpr = new MapRecord(inputStream.node.getExpression, projectFunctionDef, outputNodeId, outputNodeId, streamType)
    val outputNode = ComputedStream(outputNodeId, outputNodeId, streamExpr)
    new ObjectStream[TTarget](outputNode)
  }

  private def createProjectionFunction(fieldsNames: List[String],
                                       targetType: TypeDescriptor[_],
                                       constructorParamNames: List[String]): FunctionDef = {
    // A set of the field names that are available on the input stream.
    // We add the recordId field explicitly because even thought it might not be in the field list,
    // it's still available to the system, and we'll need it when constructing the output objects.
    val availableFieldNames = fieldsNames.toSet + RecordIdFieldName

    // First make sure all the constructor params are available in the field list.
    if (!constructorParamNames.forall(availableFieldNames.contains)) {
      val missingParams = constructorParamNames.filterNot(availableFieldNames.contains)
      throw new InvalidProgramException(s"Can't project onto type '$targetType' because these constructor parameters are missing from the input stream: $missingParams.")
    }

    val constructorArgs = constructorParamNames.map(paramName => SelectField(SelectTerm("r"), paramName))

    FunctionDef(List("r"), CreateInstance(targetType, constructorArgs))
  }

  /**
   * Gets a [[TupleStream]] that is the result of adding fields to an existing [[TupleStream]].
   * These fields are computed based on the records in the input stream.
   *
   * @param inputStream The input [[TupleStream]].
   * @param fieldsToAdd A list of [[FieldDefinition]] objects describing the fields to add.
   * @param fieldTypes  A list of [[TypeDescriptor]] objects describing the types of the fields being added.
   * @param joiner      A [[TypeJoiner]] that can provide the output type information.
   * @tparam T    The type of the input tuple stream.
   * @tparam TAdd A tuple type for the fields being added.
   * @return A [[TupleStream]] representing the input stream with the added fields.
   */
  def addFieldsToTupleStream[T <: Product, TAdd <: Product](inputStream: TupleStream[T],
                                                            fieldsToAdd: List[FieldDefinition],
                                                            fieldTypes: List[TypeDescriptor[_]],
                                                            joiner: TypeJoiner[T, TAdd]): TupleStream[joiner.OutputType] = {
    val inputRecordType = inputStream.getRecordType

    val newFields = fieldsToAdd.zip(fieldTypes)
      .map {
        case (fieldDef, fieldType) => FieldDescriptor(fieldDef.fieldName, fieldType)
      }
    val newFieldsType = new TupleTypeDescriptor[TAdd](newFields)
    val outputRecordType = joiner.getOutputType(inputRecordType, newFieldsType)

    val outputStreamType = new StreamTypeDescriptor(outputRecordType)
    val mapExpr = this.createAddFieldsMapExpression(inputStream, fieldsToAdd, outputStreamType)
    val node = ComputedStream(mapExpr.nodeId, mapExpr.nodeName, mapExpr)
    new TupleStream[joiner.OutputType](node, outputRecordType.fields)
  }

  private def createAddFieldsMapExpression(inputStream: TupleStream[_],
                                           fieldsToAdd: List[FieldDefinition],
                                           outputType: TypeDescriptor[_]): MapFields = {
    val inputRecordType = inputStream.getRecordType
    val inputExpr = inputStream.node.getStreamExpression

    val existingFields = inputRecordType.fields.map(field =>
      FieldDefinition(field.name, FunctionDef(List("r"), SelectField(SelectTerm("r"), field.name))))
    val combinedFields = existingFields ++ fieldsToAdd

    val id = Id.newId()
    new MapFields(inputExpr, combinedFields, id, id, outputType)
  }
}

package com.amazon.milan.lang.internal

import java.time.{Duration, Instant}

import com.amazon.milan.lang.{FieldStatement, GroupedStream, Stream, WindowedStream}
import com.amazon.milan.program.internal.{FilteredStreamHost, LiftableImpls}
import com.amazon.milan.program.{ComputedGraphNode, ComputedStream, ExternalStream, FieldDefinition, FunctionDef, GroupBy, LatestBy, MapFields, SelectField, SelectTerm, SlidingWindow, TumblingWindow}
import com.amazon.milan.typeutil.{FieldDescriptor, GroupedStreamTypeDescriptor, StreamTypeDescriptor, TupleTypeDescriptor, TypeDescriptor, TypeJoiner}
import com.amazon.milan.{Id, program}

import scala.reflect.macros.whitebox


/**
 * Macro bundle for operations on [[Stream]] objects.
 *
 * @param c The macro context.
 */
class StreamMacros(val c: whitebox.Context)
  extends StreamMacroHost
    with FilteredStreamHost
    with FieldStatementHost
    with LiftableImpls {

  import c.universe._

  /**
   * Creates a [[Stream]] for a record type.
   *
   * @tparam T The stream record type.
   * @return A [[Stream]] representing a stream of records of the specified type.
   */
  def of[T: c.WeakTypeTag]: c.Expr[Stream[T]] = {
    this.warnIfNoRecordId[T]()

    val weakType = c.weakTypeOf[T]
    val typeInfo = createTypeInfo[T](weakType)

    val idVal = TermName(c.freshName())
    val nodeVal = TermName(c.freshName())
    val tree =
      q"""
         val $idVal = com.amazon.milan.Id.newId()
         val $nodeVal = new ${typeOf[ExternalStream]}($idVal, $idVal, ${typeInfo.toStreamTypeDescriptor})
         new ${weakTypeOf[Stream[T]]}($nodeVal, ${typeInfo.toTypeDescriptor})
       """
    c.Expr[Stream[T]](tree)
  }

  /**
   * Creates a [[Stream]] for a tuple type with field names.
   *
   * @param fieldNames The field names corresponding to the tuple type parameters.
   * @tparam T The tuple type.
   * @return A [[Stream]] representing a stream of records of the specified type.
   */
  def ofFields[T <: Product : c.WeakTypeTag](fieldNames: c.Expr[String]*): c.Expr[Stream[T]] = {
    val recordType = this.getNamedTupleTypeDescriptor[T](fieldNames.toList)
    val streamType = q"new com.amazon.milan.typeutil.StreamTypeDescriptor($recordType)"
    val idVal = TermName(c.freshName())
    val nodeVal = TermName(c.freshName())

    val tree =
      q"""
         val $idVal = com.amazon.milan.Id.newId()
         val $nodeVal = new ${typeOf[ExternalStream]}($idVal, $idVal, $streamType)
         new ${weakTypeOf[Stream[T]]}($nodeVal, $recordType)
       """
    c.Expr[Stream[T]](tree)
  }

  /**
   * Creates a [[Stream]] that is the result of applying a filter operation.
   *
   * @param predicate The filter predicate expression.
   * @tparam T The type of the stream.
   * @return A [[Stream]] representing the filtered stream.
   */
  def where[T: c.WeakTypeTag](predicate: c.Expr[T => Boolean]): c.Expr[Stream[T]] = {
    val nodeTree = this.createdFilteredStream[T](predicate)
    val inputStreamVal = TermName(c.freshName())
    val tree =
      q"""
          val $inputStreamVal = ${c.prefix}
          new ${weakTypeOf[Stream[T]]}($nodeTree, $inputStreamVal.recordType)
       """
    c.Expr[Stream[T]](tree)
  }

  /**
   * Adds a field to a tuple stream.
   *
   * @param f      A [[FieldStatement]] describing the field to add, as computed from the existing records.
   * @param joiner A [[TypeJoiner]] that produces output type information.
   * @tparam T  The type of the input stream.
   * @tparam TF The type of the field being added.
   * @return A [[Stream]] that contains the input fields and the additional field.
   */
  def addField[T: c.WeakTypeTag, TF: c.WeakTypeTag](f: c.Expr[FieldStatement[T, TF]])
                                                   (joiner: c.Expr[TypeJoiner[T, Tuple1[TF]]]): c.universe.Tree = {
    val expr = getFieldDefinition[T, TF](f)
    this.addFields[T, Tuple1[TF]](List((expr, c.weakTypeOf[TF])))(joiner)
  }

  def addFields2[T: c.WeakTypeTag, TF1: c.WeakTypeTag, TF2: c.WeakTypeTag](f1: c.Expr[FieldStatement[T, TF1]],
                                                                           f2: c.Expr[FieldStatement[T, TF2]])
                                                                          (joiner: c.Expr[TypeJoiner[T, (TF1, TF2)]]): c.universe.Tree = {
    val expr1 = getFieldDefinition(f1)
    val expr2 = getFieldDefinition(f2)
    this.addFields[T, (TF1, TF2)](List((expr1, c.weakTypeOf[TF1]), (expr2, c.weakTypeOf[TF2])))(joiner)
  }

  def addFields3[T: c.WeakTypeTag, TF1: c.WeakTypeTag, TF2: c.WeakTypeTag, TF3: c.WeakTypeTag](f1: c.Expr[FieldStatement[T, TF1]],
                                                                                               f2: c.Expr[FieldStatement[T, TF2]],
                                                                                               f3: c.Expr[FieldStatement[T, TF3]])
                                                                                              (joiner: c.Expr[TypeJoiner[T, (TF1, TF2, TF3)]]): c.universe.Tree = {
    val expr1 = getFieldDefinition(f1)
    val expr2 = getFieldDefinition(f2)
    val expr3 = getFieldDefinition(f3)
    this.addFields[T, (TF1, TF2, TF3)](List((expr1, c.weakTypeOf[TF1]), (expr2, c.weakTypeOf[TF2]), (expr3, c.weakTypeOf[TF3])))(joiner)
  }

  private def addFields[T: c.WeakTypeTag, TAdd <: Product : c.WeakTypeTag](fields: List[(c.Expr[FieldDefinition], c.Type)])
                                                                          (joiner: c.Expr[TypeJoiner[T, TAdd]]): c.universe.Tree = {
    val leftType = c.weakTypeOf[T]
    val rightType = c.weakTypeOf[TAdd]

    val newFieldDefs = fields.map { case (f, _) => f }

    val newTupleTypeInfo = createTypeInfo[TAdd]
    val newFieldTypes = newTupleTypeInfo.genericArguments.map(_.toTypeDescriptor)

    q"com.amazon.milan.lang.internal.StreamMacroUtil.addFields[$leftType, $rightType](${c.prefix}, $newFieldDefs, $newFieldTypes, $joiner)"
  }

  /**
   * Creates a [[Stream]] from a map expression.
   */
  def map[TIn: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[TIn => TOut]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val nodeTree = createMappedToRecordStream[TIn, TOut](f)
    val outputRecordType = getTypeDescriptor[TOut]
    val tree = q"new ${weakTypeOf[Stream[TOut]]}($nodeTree, $outputRecordType)"
    c.Expr[Stream[TOut]](tree)
  }

  /**
   * Creates a [[Stream]] from one [[FieldStatement]] object.
   */
  def mapTuple1[TIn: c.WeakTypeTag, TF: c.WeakTypeTag](f: c.Expr[FieldStatement[TIn, TF]]): c.Expr[Stream[Tuple1[TF]]] = {
    val expr1 = getFieldDefinition[TIn, TF](f)
    mapTuple[Tuple1[TF]](List((expr1, c.weakTypeOf[TF])))
  }

  /**
   * Creates a [[Stream]] from two [[FieldStatement]] objects.
   */
  def mapTuple2[TIn: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag](f1: c.Expr[FieldStatement[TIn, T1]],
                                                                          f2: c.Expr[FieldStatement[TIn, T2]]): c.Expr[Stream[(T1, T2)]] = {
    val expr1 = getFieldDefinition[TIn, T1](f1)
    val expr2 = getFieldDefinition[TIn, T2](f2)
    mapTuple[(T1, T2)](List((expr1, c.weakTypeOf[T1]), (expr2, c.weakTypeOf[T2])))
  }

  /**
   * Creates a [[GroupedStream]] from a key assigner function.
   */
  def groupBy[T: c.WeakTypeTag, TKey: c.WeakTypeTag](keyFunc: c.Expr[T => TKey]): c.Expr[GroupedStream[T, TKey]] = {
    val outNodeId = Id.newId()
    val keyFuncExpr = getMilanFunction(keyFunc.tree)

    val inputNodeVal = TermName(c.freshName())
    val exprVal = TermName(c.freshName())
    val nodeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputNodeVal = ${c.prefix}.node
          val $exprVal = new ${typeOf[GroupBy]}($inputNodeVal.getStreamExpression, $keyFuncExpr, $outNodeId, $outNodeId)
          val $nodeVal = new ${typeOf[ComputedGraphNode]}($outNodeId, $exprVal)
          new ${weakTypeOf[GroupedStream[T, TKey]]}($nodeVal)
       """

    c.Expr[GroupedStream[T, TKey]](tree)
  }

  /**
   * Defines a stream of a single window that always contains the latest record to arrive for every value of a key.
   */
  def latestBy[T: c.WeakTypeTag, TKey: c.WeakTypeTag](dateExtractor: c.Expr[T => Instant],
                                                      keyFunc: c.Expr[T => TKey]): c.Expr[WindowedStream[T]] = {
    val dateExtractorExpr = getMilanFunction(dateExtractor.tree)
    val keyFuncExpr = getMilanFunction(keyFunc.tree)

    val inputStreamVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputStreamVal = ${c.prefix}
          com.amazon.milan.lang.internal.StreamMacroUtil.latestBy[${c.weakTypeOf[T]}, ${c.weakTypeOf[TKey]}]($inputStreamVal, $dateExtractorExpr, $keyFuncExpr)
       """

    c.Expr[WindowedStream[T]](tree)
  }

  /**
   * Creates a [[WindowedStream]] from a stream given window parameters.
   */
  def tumblingWindow[T: c.WeakTypeTag](dateExtractor: c.Expr[T => Instant],
                                       windowPeriod: c.Expr[Duration],
                                       offset: c.Expr[Duration]): c.Expr[WindowedStream[T]] = {
    val outNodeId = Id.newId()
    val dateExtractorFunc = getMilanFunction(dateExtractor.tree)

    val periodExpr = this.getDuration(windowPeriod)
    val offsetExpr = this.getDuration(offset)

    val inputNodeVal = TermName(c.freshName())
    val exprVal = TermName(c.freshName())
    val nodeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputNodeVal = ${c.prefix}.node
          val $exprVal = new ${typeOf[TumblingWindow]}($inputNodeVal.getExpression, $dateExtractorFunc, $periodExpr, $offsetExpr, $outNodeId, $outNodeId)
          val $nodeVal = new ${typeOf[ComputedGraphNode]}($outNodeId, $exprVal)
          new ${weakTypeOf[WindowedStream[T]]}($nodeVal)
       """

    c.Expr[WindowedStream[T]](tree)
  }

  /**
   * Creates a [[WindowedStream]] from a stream given window parameters.
   */
  def slidingWindow[T: c.WeakTypeTag](dateExtractor: c.Expr[T => Instant],
                                      windowSize: c.Expr[Duration],
                                      slide: c.Expr[Duration],
                                      offset: c.Expr[Duration]): c.Expr[WindowedStream[T]] = {
    val outNodeId = Id.newId()
    val dateExtractorFunc = getMilanFunction(dateExtractor.tree)

    val sizeExpr = this.getDuration(windowSize)
    val slideExpr = this.getDuration(slide)
    val offsetExpr = this.getDuration(offset)

    val inputNodeVal = TermName(c.freshName())
    val exprVal = TermName(c.freshName())
    val nodeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputNodeVal = ${c.prefix}.node
          val $exprVal = new ${typeOf[SlidingWindow]}($inputNodeVal.getExpression, $dateExtractorFunc, $sizeExpr, $slideExpr, $offsetExpr, $outNodeId, $outNodeId)
          val $nodeVal = new ${typeOf[ComputedGraphNode]}($outNodeId, $exprVal)
          new ${weakTypeOf[WindowedStream[T]]}($nodeVal)
       """

    c.Expr[WindowedStream[T]](tree)
  }

  private def getDuration(duration: c.Expr[Duration]): c.Expr[program.Duration] = {
    c.Expr[program.Duration](q"new ${typeOf[program.Duration]}($duration.toMillis)")
  }
}


object StreamMacroUtil {
  def latestBy[T, TKey](inputStream: Stream[T],
                        dateExtractor: FunctionDef,
                        keyFunc: FunctionDef): WindowedStream[T] = {
    val nodeId = Id.newId()
    val inputRecordType = inputStream.recordType
    val outputStreamType = new GroupedStreamTypeDescriptor(inputRecordType)
    val latestExpr = new LatestBy(inputStream.node.getStreamExpression, dateExtractor, keyFunc, nodeId, nodeId, outputStreamType)

    val node = ComputedGraphNode(nodeId, latestExpr)
    new WindowedStream[T](node)
  }

  /**
   * Gets a [[Stream]] that is the result of adding fields to an existing [[Stream]].
   * These fields are computed based on the records in the input stream.
   *
   * @param inputStream The input [[Stream]].
   * @param fieldsToAdd A list of [[FieldDefinition]] objects describing the fields to add.
   * @param fieldTypes  A list of [[TypeDescriptor]] objects describing the types of the fields being added.
   * @param joiner      A [[TypeJoiner]] that can provide the output type information.
   * @tparam T    The type of the input tuple stream.
   * @tparam TAdd A tuple type for the fields being added.
   * @return A [[Stream]] representing the input stream with the added fields.
   */
  def addFields[T <: Product, TAdd <: Product](inputStream: Stream[T],
                                               fieldsToAdd: List[FieldDefinition],
                                               fieldTypes: List[TypeDescriptor[_]],
                                               joiner: TypeJoiner[T, TAdd]): Stream[joiner.OutputType] = {
    val inputRecordType = inputStream.recordType

    val newFields = fieldsToAdd.zip(fieldTypes)
      .map {
        case (fieldDef, fieldType) => FieldDescriptor(fieldDef.fieldName, fieldType)
      }
    val newFieldsType = new TupleTypeDescriptor[TAdd](newFields)
    val outputRecordType = joiner.getOutputType(inputRecordType, newFieldsType)

    val outputStreamType = new StreamTypeDescriptor(outputRecordType)
    val mapExpr = this.createAddFieldsMapExpression(inputStream, fieldsToAdd, outputStreamType)
    val node = ComputedStream(mapExpr.nodeId, mapExpr.nodeName, mapExpr)
    new Stream[joiner.OutputType](node, outputRecordType)
  }

  private def createAddFieldsMapExpression(inputStream: Stream[_],
                                           fieldsToAdd: List[FieldDefinition],
                                           outputType: TypeDescriptor[_]): MapFields = {
    val inputRecordType = inputStream.recordType
    val inputExpr = inputStream.node.getStreamExpression

    val existingFields = inputRecordType.fields.map(field =>
      FieldDefinition(field.name, FunctionDef(List("r"), SelectField(SelectTerm("r"), field.name))))
    val combinedFields = existingFields ++ fieldsToAdd

    val id = Id.newId()
    new MapFields(inputExpr, combinedFields, id, id, outputType)
  }
}

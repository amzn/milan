package com.amazon.milan.lang.internal

import com.amazon.milan.lang.{GroupedStream, Stream, TimeWindowedStream}
import com.amazon.milan.program.internal.{FilteredStreamHost, LiftableImpls}
import com.amazon.milan.program.{ExternalStream, FunctionDef, GroupBy, NamedField, NamedFields, SelectField, SelectTerm, SlidingWindow, StreamArgMax, StreamArgMin, StreamMap, SumBy, TumblingWindow, ValueDef}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, FieldDescriptor, GroupedStreamTypeDescriptor, TupleTypeDescriptor, TypeDescriptor, TypeJoiner}
import com.amazon.milan.{Id, program}

import java.time.{Duration, Instant}
import scala.reflect.macros.whitebox


/**
 * Macro bundle for operations on [[Stream]] objects.
 *
 * @param c The macro context.
 */
class StreamMacros(val c: whitebox.Context)
  extends StreamMacroHost
    with FilteredStreamHost
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

    val idVal = TermName(c.freshName("id"))
    val exprVal = TermName(c.freshName("expr"))
    val tree =
      q"""
         val $idVal = com.amazon.milan.Id.newId()
         val $exprVal = new ${typeOf[ExternalStream]}($idVal, $idVal, ${typeInfo.toStreamTypeDescriptor})
         new ${weakTypeOf[Stream[T]]}($exprVal, ${typeInfo.toTypeDescriptor})
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
    val streamType = q"new ${typeOf[DataStreamTypeDescriptor]}($recordType)"
    val idVal = TermName(c.freshName("id"))
    val exprVal = TermName(c.freshName("expr"))

    val tree =
      q"""
         val $idVal = com.amazon.milan.Id.newId()
         val $exprVal = new ${typeOf[ExternalStream]}($idVal, $idVal, $streamType)
         new ${weakTypeOf[Stream[T]]}($exprVal, $recordType)
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
    val inputStreamVal = TermName(c.freshName("inputStream"))
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
   * @param f      A function that creates the fields to add, as computed from the existing records.
   * @param joiner A [[TypeJoiner]] that produces output type information.
   * @tparam T  The type of the input stream.
   * @tparam TF The type of the field being added.
   * @return A [[Stream]] that contains the input fields and the additional field.
   */
  def addFields[T: c.WeakTypeTag, TF <: Product : c.WeakTypeTag](f: c.Expr[T => TF])
                                                                (joiner: c.Expr[TypeJoiner[T, TF]]): c.universe.Tree = {
    val newFieldsFunction = getMilanFunction(f.tree)
    val leftType = c.weakTypeOf[T]
    val rightType = c.weakTypeOf[TF]
    val rightFieldTypes = getTypeDescriptor(rightType).genericArguments

    q"com.amazon.milan.lang.internal.StreamMacroUtil.addFields[$leftType, $rightType](${c.prefix}, $newFieldsFunction, $rightFieldTypes, $joiner)"
  }

  /**
   * Creates a [[Stream]] from a map expression.
   */
  def map[TIn: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[TIn => TOut]): c.Expr[Stream[TOut]] = {
    this.warnIfNoRecordId[TOut]()

    val exprTree = createMappedToRecordStream[TIn, TOut](f)
    val exprVal = TermName(c.freshName("expr"))
    val tree =
      q"""
          val $exprVal = $exprTree
          new ${weakTypeOf[Stream[TOut]]}($exprVal, $exprVal.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[TOut]]}])
       """
    c.Expr[Stream[TOut]](tree)
  }

  /**
   * Creates a [[GroupedStream]] from a key assigner function.
   */
  def groupBy[T: c.WeakTypeTag, TKey: c.WeakTypeTag](keyFunc: c.Expr[T => TKey]): c.Expr[GroupedStream[T, TKey]] = {
    val outNodeId = Id.newId()
    val keyFuncExpr = getMilanFunction(keyFunc.tree)
    val keyType = getTypeDescriptor[TKey]

    val inputExprVal = TermName(c.freshName("inputExpr"))
    val exprVal = TermName(c.freshName("expr"))
    val exprTypeVal = TermName(c.freshName("exprType"))

    val tree =
      q"""
          val $inputExprVal = ${c.prefix}.expr
          val $exprTypeVal = new ${typeOf[GroupedStreamTypeDescriptor]}($keyType, $inputExprVal.recordType)
          val $exprVal = new ${typeOf[GroupBy]}($inputExprVal, $keyFuncExpr, $outNodeId, $outNodeId, $exprTypeVal)
          new ${weakTypeOf[GroupedStream[T, TKey]]}($exprVal)
       """

    c.Expr[GroupedStream[T, TKey]](tree)
  }

  /**
   * Creates a [[TimeWindowedStream]] from a stream given window parameters.
   */
  def tumblingWindow[T: c.WeakTypeTag](dateExtractor: c.Expr[T => Instant],
                                       windowPeriod: c.Expr[Duration],
                                       offset: c.Expr[Duration]): c.Expr[TimeWindowedStream[T]] = {
    val outNodeId = Id.newId()
    val dateExtractorFunc = getMilanFunction(dateExtractor.tree)
    val keyType = getTypeDescriptor[Instant]

    val periodExpr = this.getDuration(windowPeriod)
    val offsetExpr = this.getDuration(offset)

    val inputExprVal = TermName(c.freshName("inputExpr"))
    val outputTypeVal = TermName(c.freshName("outputType"))
    val exprVal = TermName(c.freshName("expr"))

    val tree =
      q"""
          val $inputExprVal = ${c.prefix}.expr
          val $outputTypeVal = new ${typeOf[GroupedStreamTypeDescriptor]}($keyType, $inputExprVal.recordType)
          val $exprVal = new ${typeOf[TumblingWindow]}($inputExprVal, $dateExtractorFunc, $periodExpr, $offsetExpr, $outNodeId, $outNodeId, $outputTypeVal)
          new ${weakTypeOf[TimeWindowedStream[T]]}($exprVal)
       """

    c.Expr[TimeWindowedStream[T]](tree)
  }

  /**
   * Creates a [[TimeWindowedStream]] from a stream given window parameters.
   */
  def slidingWindow[T: c.WeakTypeTag](dateExtractor: c.Expr[T => Instant],
                                      windowSize: c.Expr[Duration],
                                      slide: c.Expr[Duration],
                                      offset: c.Expr[Duration]): c.Expr[TimeWindowedStream[T]] = {
    val outNodeId = Id.newId()
    val dateExtractorFunc = getMilanFunction(dateExtractor.tree)
    val keyType = getTypeDescriptor[Instant]

    val sizeExpr = this.getDuration(windowSize)
    val slideExpr = this.getDuration(slide)
    val offsetExpr = this.getDuration(offset)

    val inputExprVal = TermName(c.freshName("inputExpr"))
    val outputTypeVal = TermName(c.freshName("outputType"))
    val exprVal = TermName(c.freshName("expr"))

    val tree =
      q"""
          val $inputExprVal = ${c.prefix}.expr
          val $outputTypeVal = new ${typeOf[GroupedStreamTypeDescriptor]}($keyType, $inputExprVal.recordType)
          val $exprVal = new ${typeOf[SlidingWindow]}($inputExprVal, $dateExtractorFunc, $sizeExpr, $slideExpr, $offsetExpr, $outNodeId, $outNodeId, $outputTypeVal)
          new ${weakTypeOf[TimeWindowedStream[T]]}($exprVal)
       """

    c.Expr[TimeWindowedStream[T]](tree)
  }

  private def getDuration(duration: c.Expr[Duration]): c.Expr[program.Duration] = {
    c.Expr[program.Duration](q"new ${typeOf[program.Duration]}($duration.toMillis)")
  }

  /**
   * Creates a [[Stream]] of the argmax of an input stream.
   */
  def maxBy[T: c.WeakTypeTag, TArg: c.WeakTypeTag](argExtractor: c.Expr[T => TArg]): c.Expr[Stream[T]] = {
    val argExtractorExpr = getMilanFunction(argExtractor.tree)

    val inputStreamVal = TermName(c.freshName("inputStream"))
    val exprVal = TermName(c.freshName("expr"))
    val id = Id.newId()

    val tree =
      q"""
          val $inputStreamVal = ${c.prefix}
          val $exprVal = new ${typeOf[StreamArgMax]}($inputStreamVal.expr, $argExtractorExpr, $id, $id, $inputStreamVal.expr.tpe)
          new ${weakTypeOf[Stream[T]]}($exprVal, $inputStreamVal.expr.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[T]]}])
       """

    c.Expr[Stream[T]](tree)
  }

  /**
   * Creates a [[Stream]] of the argmin of an input stream.
   */
  def minBy[T: c.WeakTypeTag, TArg: c.WeakTypeTag](argExtractor: c.Expr[T => TArg]): c.Expr[Stream[T]] = {
    val argExtractorExpr = getMilanFunction(argExtractor.tree)

    val inputStreamVal = TermName(c.freshName("inputStream"))
    val exprVal = TermName(c.freshName("expr"))
    val id = Id.newId()

    val tree =
      q"""
          val $inputStreamVal = ${c.prefix}
          val $exprVal = new ${typeOf[StreamArgMin]}($inputStreamVal.expr, $argExtractorExpr, $id, $id, $inputStreamVal.expr.tpe)
          new ${weakTypeOf[Stream[T]]}($exprVal, $inputStreamVal.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[T]]}])
       """

    c.Expr[Stream[T]](tree)
  }

  /**
   * Creates a [[Stream]] based on the cumulative sum of values computed from the input records.
   */
  def sumBy[T: c.WeakTypeTag, TArg: c.WeakTypeTag, TOut: c.WeakTypeTag](argExtractor: c.Expr[T => TArg],
                                                                        createOutput: c.Expr[(T, TArg) => TOut]): c.Expr[Stream[TOut]] = {
    val argExpr = getMilanFunction(argExtractor.tree)
    val outputExpr = getMilanFunction(createOutput.tree)

    val inputStreamVal = TermName(c.freshName("inputStream"))
    val exprVal = TermName(c.freshName("expr"))
    val streamType = getStreamTypeExpr[TOut](outputExpr)
    val streamTypeVal = TermName(c.freshName("streamType"))
    val id = Id.newId()

    val tree =
      q"""
          val $inputStreamVal = ${c.prefix}
          val $streamTypeVal = $streamType
          val $exprVal = new ${typeOf[SumBy]}($inputStreamVal.expr, $argExpr, $outputExpr, $id, $id, $streamTypeVal)
          new ${weakTypeOf[Stream[TOut]]}($exprVal, $streamTypeVal.recordType.asInstanceOf[${weakTypeOf[TypeDescriptor[TOut]]}])
       """

    c.Expr[Stream[TOut]](tree)
  }
}


object StreamMacroUtil {
  /**
   * Gets a [[Stream]] that is the result of adding fields to an existing [[Stream]].
   * These fields are computed based on the records in the input stream.
   *
   * @param inputStream       The input [[Stream]].
   * @param newFieldsFunction A [[FunctionDef]] that produces the tuple of new fields.
   * @param joiner            A [[TypeJoiner]] that can provide the output type information.
   * @tparam T    The type of the input tuple stream.
   * @tparam TAdd A tuple type for the fields being added.
   * @return A [[Stream]] representing the input stream with the added fields.
   */
  def addFields[T <: Product, TAdd <: Product](inputStream: Stream[T],
                                               newFieldsFunction: FunctionDef,
                                               newFieldTypes: List[TypeDescriptor[_]],
                                               joiner: TypeJoiner[T, TAdd]): Stream[joiner.OutputType] = {
    val inputRecordType = inputStream.recordType

    val FunctionDef(List(ValueDef(inputTermName, _)), NamedFields(fieldsToAdd)) = newFieldsFunction

    val newFields = fieldsToAdd.zip(newFieldTypes).map {
      case (field, fieldType) => FieldDescriptor(field.fieldName, fieldType)
    }

    val newFieldsTupleType = new TupleTypeDescriptor[TAdd](newFields)
    val outputRecordType = joiner.getOutputType(inputRecordType, newFieldsTupleType)

    val outputStreamType = new DataStreamTypeDescriptor(outputRecordType)
    val mapExpr = this.createAddFieldsMapExpression(inputStream, fieldsToAdd, inputTermName, outputStreamType)
    new Stream[joiner.OutputType](mapExpr, outputRecordType)
  }

  private def createAddFieldsMapExpression(inputStream: Stream[_],
                                           fieldsToAdd: List[NamedField],
                                           inputTermName: String,
                                           outputType: TypeDescriptor[_]): StreamMap = {
    val inputRecordType = inputStream.recordType
    val inputExpr = inputStream.expr

    val existingFields = inputRecordType.fields.map(field =>
      NamedField(field.name, SelectField(SelectTerm(inputTermName), field.name)))
    val combinedFields = existingFields ++ fieldsToAdd

    val mapFunction = FunctionDef(List(ValueDef(inputTermName, inputRecordType)), NamedFields(combinedFields))
    val id = Id.newId()
    new StreamMap(inputExpr, mapFunction, id, id, outputType)
  }
}

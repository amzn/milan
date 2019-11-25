package com.amazon.milan.lang.internal

import java.time.{Duration, Instant}

import com.amazon.milan.lang.{FieldStatement, GroupedStream, ObjectStream, Stream, TupleStream, WindowedStream}
import com.amazon.milan.program.internal.LiftableImpls
import com.amazon.milan.program.{ComputedGraphNode, ExternalStream, FunctionDef, GroupBy, LatestBy, SlidingWindow, TumblingWindow}
import com.amazon.milan.types.Record
import com.amazon.milan.typeutil.GroupedStreamTypeDescriptor
import com.amazon.milan.{Id, program}

import scala.reflect.macros.whitebox


/**
 * Macro bundle for operations on [[Stream]] objects.
 *
 * @param c The macro context.
 */
class StreamMacros(val c: whitebox.Context)
  extends StreamMacroHost
    with FieldStatementHost
    with LiftableImpls
    with LangTypeNamesHost {

  import c.universe._

  /**
   * Creates an [[ObjectStream]] for a record type.
   *
   * @tparam T The stream record type.
   * @return An [[ObjectStream]] representing a stream of records of the specified type.
   */
  def of[T <: Record : c.WeakTypeTag]: c.Expr[ObjectStream[T]] = {
    val weakType = c.weakTypeOf[T]
    val typeInfo = createTypeInfo[T](weakType)

    val idVal = TermName(c.freshName())
    val nodeVal = TermName(c.freshName())
    val tree =
      q"""
         val $idVal = com.amazon.milan.Id.newId()
         val $nodeVal = new ${typeOf[ExternalStream]}($idVal, $idVal, ${typeInfo.toStreamTypeDescriptor})
         new ${objectStreamTypeName(weakType)}($nodeVal)
       """
    c.Expr[ObjectStream[T]](tree)
  }

  /**
   * Creates a [[TupleStream]] for a tuple type with field names.
   *
   * @param fieldNames The field names corresponding to the tuple type parameters.
   * @tparam T The tuple type.
   * @return A [[TupleStream]] representing a stream of records of the specified type.
   */
  def ofFields[T <: Product : c.WeakTypeTag](fieldNames: c.Expr[String]*): c.Expr[TupleStream[T]] = {
    val recordType = this.getNamedTupleTypeDescriptor[T](fieldNames.toList)
    val streamType = q"new com.amazon.milan.typeutil.StreamTypeDescriptor($recordType)"
    val idVal = TermName(c.freshName())
    val nodeVal = TermName(c.freshName())
    val tree =
      q"""
         val $idVal = com.amazon.milan.Id.newId()
         val $nodeVal = new ${typeOf[ExternalStream]}($idVal, $idVal, $streamType)
         new ${tupleStreamTypeName(c.weakTypeOf[T])}($nodeVal, $recordType.fields)
       """
    c.Expr[TupleStream[T]](tree)
  }

  /**
   * Creates an [[ObjectStream]] from a map expression.
   */
  def map[TIn: c.WeakTypeTag, TOut <: Record : c.WeakTypeTag](f: c.Expr[TIn => TOut]): c.Expr[ObjectStream[TOut]] = {
    val nodeTree = createMappedToRecordStream[TIn, TOut](f)
    val outputType = c.weakTypeOf[TOut]
    val tree = q"new ${objectStreamTypeName(outputType)}($nodeTree)"
    c.Expr[ObjectStream[TOut]](tree)
  }

  /**
   * Creates a [[TupleStream]] from one [[FieldStatement]] objects.
   */
  def mapTuple1[TIn: c.WeakTypeTag, TF: c.WeakTypeTag](f: c.Expr[FieldStatement[TIn, TF]]): c.Expr[TupleStream[Tuple1[TF]]] = {
    val expr1 = getFieldDefinition[TIn, TF](f)
    mapTuple[Tuple1[TF]](List((expr1, c.weakTypeOf[TF])))
  }

  /**
   * Creates a [[TupleStream]] from two [[FieldStatement]] objects.
   */
  def mapTuple2[TIn: c.WeakTypeTag, T1: c.WeakTypeTag, T2: c.WeakTypeTag](f1: c.Expr[FieldStatement[TIn, T1]],
                                                                          f2: c.Expr[FieldStatement[TIn, T2]]): c.Expr[TupleStream[(T1, T2)]] = {
    val expr1 = getFieldDefinition[TIn, T1](f1)
    val expr2 = getFieldDefinition[TIn, T2](f2)
    mapTuple[(T1, T2)](List((expr1, c.weakTypeOf[T1]), (expr2, c.weakTypeOf[T2])))
  }

  /**
   * Creates a [[GroupedStream]] from a key assigner function.
   */
  def groupBy[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TStream <: Stream[T, _] : c.WeakTypeTag](keyFunc: c.Expr[T => TKey]): c.Expr[GroupedStream[T, TKey, TStream]] = {
    val outNodeId = Id.newId()
    val recordType = c.weakTypeOf[T]
    val keyType = c.weakTypeOf[TKey]
    val keyFuncExpr = getMilanFunction(keyFunc.tree)
    val streamType = c.weakTypeOf[TStream]

    val inputNodeVal = TermName(c.freshName())
    val exprVal = TermName(c.freshName())
    val nodeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputNodeVal = ${c.prefix}.node
          val $exprVal = new ${typeOf[GroupBy]}($inputNodeVal.getStreamExpression, $keyFuncExpr, $outNodeId, $outNodeId)
          val $nodeVal = new ${typeOf[ComputedGraphNode]}($outNodeId, $exprVal)
          new ${groupedStreamTypeName(recordType, keyType, streamType)}($nodeVal)
       """

    c.Expr[GroupedStream[T, TKey, TStream]](tree)
  }

  /**
   * Defines a stream of a single window that always contains the latest record to arrive for every value of a key.
   */
  def latestBy[T: c.WeakTypeTag, TKey: c.WeakTypeTag, TStream <: Stream[T, _] : c.WeakTypeTag](dateExtractor: c.Expr[T => Instant],
                                                                                               keyFunc: c.Expr[T => TKey]): c.Expr[WindowedStream[T, TStream]] = {
    val dateExtractorExpr = getMilanFunction(dateExtractor.tree)
    val keyFuncExpr = getMilanFunction(keyFunc.tree)

    val inputStreamVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputStreamVal = ${c.prefix}
          com.amazon.milan.lang.internal.StreamMacroUtil.latestBy[${c.weakTypeOf[T]}, ${c.weakTypeOf[TKey]}, ${c.weakTypeOf[TStream]}]($inputStreamVal, $dateExtractorExpr, $keyFuncExpr)
       """

    c.Expr[WindowedStream[T, TStream]](tree)
  }

  /**
   * Creates a [[WindowedStream]] from a stream given window parameters.
   */
  def tumblingWindow[T: c.WeakTypeTag, TStream <: Stream[T, _] : c.WeakTypeTag](dateExtractor: c.Expr[T => Instant],
                                                                                windowPeriod: c.Expr[Duration],
                                                                                offset: c.Expr[Duration]): c.Expr[WindowedStream[T, TStream]] = {
    val outNodeId = Id.newId()
    val recordType = c.weakTypeOf[T]
    val dateExtractorFunc = getMilanFunction(dateExtractor.tree)
    val streamType = c.weakTypeOf[TStream]

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
          new ${windowedStreamTypeName(recordType, streamType)}($nodeVal)
       """

    c.Expr[WindowedStream[T, TStream]](tree)
  }

  /**
   * Creates a [[WindowedStream]] from a stream given window parameters.
   */
  def slidingWindow[T: c.WeakTypeTag, TStream <: Stream[T, _] : c.WeakTypeTag](dateExtractor: c.Expr[T => Instant],
                                                                               windowSize: c.Expr[Duration],
                                                                               slide: c.Expr[Duration],
                                                                               offset: c.Expr[Duration]): c.Expr[WindowedStream[T, TStream]] = {
    val outNodeId = Id.newId()
    val recordType = c.weakTypeOf[T]
    val dateExtractorFunc = getMilanFunction(dateExtractor.tree)
    val streamType = c.weakTypeOf[TStream]

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
          new ${windowedStreamTypeName(recordType, streamType)}($nodeVal)
       """

    c.Expr[WindowedStream[T, TStream]](tree)
  }

  private def getDuration(duration: c.Expr[Duration]): c.Expr[program.Duration] = {
    c.Expr[program.Duration](q"new ${typeOf[program.Duration]}($duration.toMillis)")
  }
}


object StreamMacroUtil {
  def latestBy[T, TKey, TStream <: Stream[T, _]](inputStream: Stream[T, TStream],
                                                 dateExtractor: FunctionDef,
                                                 keyFunc: FunctionDef): WindowedStream[T, TStream] = {
    val nodeId = Id.newId()
    val inputRecordType = inputStream.getRecordType
    val outputStreamType = new GroupedStreamTypeDescriptor(inputRecordType)
    val latestExpr = new LatestBy(inputStream.node.getStreamExpression, dateExtractor, keyFunc, nodeId, nodeId, outputStreamType)

    val node = ComputedGraphNode(nodeId, latestExpr)
    new WindowedStream[T, TStream](node)
  }
}
package com.amazon.milan.program.internal

import com.amazon.milan.Id
import com.amazon.milan.program._
import com.amazon.milan.typeutil.{StreamTypeDescriptor, TypeDescriptor}

import scala.reflect.macros.whitebox

/**
 * Trait enabling macro bundles to create [[ComputedStream]] graph nodes that have [[MapRecord]] and [[MapFields]]
 * computation expressions.
 */
trait MappedStreamHost extends ConvertExpressionHost with ProgramTypeNamesHost {
  val c: whitebox.Context

  import c.universe._

  /**
   * Creates an expression that evaluates to a [[ComputedStream]] object, for a map operation that yields a record stream.
   *
   * @param f A map function expression.
   * @tparam TIn  The input type of the map function.
   * @tparam TOut The output type of the map function.
   * @return An expression that evaluates to a [[ComputedStream]] object.
   */
  def createMappedToRecordStream[TIn: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[TIn => TOut]): c.Expr[ComputedStream] = {
    val mapExpression = getMilanFunction(f.tree)
    val validationExpression = q"com.amazon.milan.program.ProgramValidation.validateMapFunction($mapExpression, 1)"
    this.createMapRecordStream[TOut](mapExpression, validationExpression)
  }

  /**
   * Creates an expression that evaluates to a [[ComputedStream]] object, for a map operation with two input streams that
   * yields a record stream.
   *
   * @param f A map function expression.
   * @tparam T1   The type of the first argument of the map function.
   * @tparam T2   The type of the second argument of the map function.
   * @tparam TOut The output type of the map function.
   * @return An expression that evaluates to a [[ComputedStream]] object.
   */
  def createMappedToRecordStream2[T1: c.WeakTypeTag, T2: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(T1, T2) => TOut],
                                                                                             validationExpressionGenerator: c.Expr[FunctionDef] => c.universe.Tree = null): c.Expr[ComputedStream] = {
    val mapExpression = getMilanFunction(f.tree)

    val validationExpression =
      if (validationExpressionGenerator == null) {
        q"com.amazon.milan.program.ProgramValidation.validateMapFunction($mapExpression, 2)"
      }
      else {
        q"""
            com.amazon.milan.program.ProgramValidation.validateMapFunction($mapExpression, 2)
            ${validationExpressionGenerator(mapExpression)}
         """
      }

    this.createMapRecordStream[TOut](mapExpression, validationExpression)
  }

  /**
   * Creates an expression that evaluates to a [[ComputedStream]] object, for a map operation that yields a record stream.
   *
   * @param mapExpression The [[FunctionDef]] containing the map expression.
   * @tparam TOut The output type of the map function.
   * @return An expression that evaluates to a [[ComputedStream]] object.
   */
  private def createMapRecordStream[TOut: c.WeakTypeTag](mapExpression: c.Expr[FunctionDef],
                                                         validationExpression: c.universe.Tree): c.Expr[ComputedStream] = {
    val outputTypeInfo = createTypeInfo[TOut]
    val outputNodeId = Id.newId()

    val inputNodeVal = TermName(c.freshName())
    val sourceExpressionVal = TermName(c.freshName())
    val mapExpressionVal = TermName(c.freshName())

    val tree =
      q"""
          $validationExpression

          val $inputNodeVal = ${c.prefix}.node
          val $sourceExpressionVal = $inputNodeVal.getExpression
          val $mapExpressionVal = new ${typeOf[MapRecord]}($sourceExpressionVal, $mapExpression, $outputNodeId, $outputNodeId, ${outputTypeInfo.toStreamTypeDescriptor})
          new ${typeOf[ComputedStream]}($outputNodeId, $outputNodeId, $mapExpressionVal)
       """
    c.Expr[ComputedStream](tree)
  }

  /**
   * Creates an expression that evaluates to a [[ComputedStream]] object, for a map operation that yields a tuple stream.
   *
   * @param outputRecordType A [[TypeDescriptor]] describing the output tuple type.
   * @param fields           A list of expression trees that evaluate to [[FieldDefinition]] objects.
   * @tparam TOut The output stream type.
   * @return An expression that evaluates to a [[ComputedStream]] object.
   */
  def createMappedToTupleStream[TOut <: Product : c.WeakTypeTag](outputRecordType: c.Expr[TypeDescriptor[TOut]],
                                                                 fields: List[c.Expr[FieldDefinition]]): c.Expr[ComputedStream] = {
    val outputNodeId = Id.newId()

    val inputNodeVal = TermName(c.freshName())
    val sourceExpressionVal = TermName(c.freshName())
    val mapExpressionVal = TermName(c.freshName())
    val streamTypeVal = TermName(c.freshName())

    val tree =
      q"""
          val $inputNodeVal = ${c.prefix}.node
          val $sourceExpressionVal = $inputNodeVal.getExpression
          val $streamTypeVal = new ${typeOf[StreamTypeDescriptor]}($outputRecordType)
          val $mapExpressionVal = new ${typeOf[MapFields]}($sourceExpressionVal, List(..$fields), $outputNodeId, $outputNodeId, $streamTypeVal)
          new ${typeOf[ComputedStream]}($outputNodeId, $outputNodeId, $mapExpressionVal)
       """
    c.Expr[ComputedStream](tree)
  }
}

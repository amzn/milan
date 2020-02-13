package com.amazon.milan.program.internal

import com.amazon.milan.Id
import com.amazon.milan.program._
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, TypeDescriptor}

import scala.reflect.macros.whitebox

/**
 * Trait enabling macro bundles to create [[MapRecord]] and [[MapFields]] expressions.
 */
trait MappedStreamHost extends ConvertExpressionHost with ProgramTypeNamesHost {
  val c: whitebox.Context

  import c.universe._

  /**
   * Creates an expression that evaluates to a [[MapRecord]] object, for a map operation that yields a record stream.
   *
   * @param f A map function expression.
   * @tparam TIn  The input type of the map function.
   * @tparam TOut The output type of the map function.
   * @return An expression that evaluates to a [[MapRecord]] object.
   */
  def createMappedToRecordStream[TIn: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[TIn => TOut]): c.Expr[MapRecord] = {
    val mapExpression = getMilanFunction(f.tree)
    val validationExpression = q"com.amazon.milan.program.ProgramValidation.validateMapFunction($mapExpression, 1)"
    this.createMapRecord[TOut](mapExpression, validationExpression)
  }

  /**
   * Creates an expression that evaluates to a [[StreamExpression]] object, for a map operation with two input streams that
   * yields a record stream.
   *
   * @param f A map function expression.
   * @tparam T1   The type of the first argument of the map function.
   * @tparam T2   The type of the second argument of the map function.
   * @tparam TOut The output type of the map function.
   * @return An expression that evaluates to a [[StreamExpression]] object.
   */
  def createMappedToRecordStream2[T1: c.WeakTypeTag, T2: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(T1, T2) => TOut],
                                                                                             validationExpressionGenerator: c.Expr[FunctionDef] => c.universe.Tree = null): c.Expr[StreamExpression] = {
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

    this.createMapRecord[TOut](mapExpression, validationExpression)
  }

  /**
   * Creates an expression that evaluates to a [[MapRecord]] object, for a map operation that yields a record stream.
   *
   * @param mapExpression The [[FunctionDef]] containing the map expression.
   * @tparam TOut The output type of the map function.
   * @return An expression that evaluates to a [[MapRecord]] object.
   */
  private def createMapRecord[TOut: c.WeakTypeTag](mapExpression: c.Expr[FunctionDef],
                                                   validationExpression: c.universe.Tree): c.Expr[MapRecord] = {
    val outputTypeInfo = createTypeInfo[TOut]
    val outputNodeId = Id.newId()

    val sourceExpressionVal = TermName(c.freshName())

    val tree =
      q"""
          $validationExpression

          val $sourceExpressionVal = ${c.prefix}.expr
          new ${typeOf[MapRecord]}($sourceExpressionVal, $mapExpression, $outputNodeId, $outputNodeId, ${outputTypeInfo.toStreamTypeDescriptor})
       """
    c.Expr[MapRecord](tree)
  }

  /**
   * Creates an expression that evaluates to a [[MapFields]] object, for a map operation that yields a tuple stream.
   *
   * @param outputRecordType A [[TypeDescriptor]] describing the output tuple type.
   * @param fields           A list of expression trees that evaluate to [[FieldDefinition]] objects.
   * @tparam TOut The output stream type.
   * @return An expression that evaluates to a [[MapFields]] object.
   */
  def createMappedToTupleStream[TOut <: Product : c.WeakTypeTag](outputRecordType: c.Expr[TypeDescriptor[TOut]],
                                                                 fields: List[c.Expr[FieldDefinition]]): c.Expr[MapFields] = {
    val outputNodeId = Id.newId()

    val sourceExpressionVal = TermName(c.freshName())
    val streamTypeVal = TermName(c.freshName())

    val tree =
      q"""
          val $sourceExpressionVal = ${c.prefix}.expr
          val $streamTypeVal = new ${typeOf[DataStreamTypeDescriptor]}($outputRecordType)
          new ${typeOf[MapFields]}($sourceExpressionVal, List(..$fields), $outputNodeId, $outputNodeId, $streamTypeVal)
       """
    c.Expr[MapFields](tree)
  }
}

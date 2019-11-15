package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.RuntimeEvaluator
import com.amazon.milan.program.FunctionDef
import com.amazon.milan.program.internal.ConvertExpressionHost
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.language.experimental.macros
import scala.reflect.macros.whitebox


class RuntimeCompiledFunctionException(message: String, val functionDef: String, cause: Throwable)
  extends Exception(message + " Function definition: " + functionDef, cause)


trait SerializableFunction[TIn, +TOut] extends Serializable {
  def apply(in: TIn): TOut
}


trait SerializableFunction2[T1, T2, +TOut] extends Serializable {
  def apply(in1: T1, in2: T2): TOut
}


class ConstantFunction2[T1, T2, +TOut](value: TOut) extends SerializableFunction2[T1, T2, TOut] {
  override def apply(in1: T1, in2: T2): TOut = value
}


class RuntimeCompiledFunction[TIn, +TOut](functionDef: String)
  extends SerializableFunction[TIn, TOut] {

  @transient private lazy val compiledFunction = this.compileFunction()

  def this(inputType: TypeDescriptor[_], f: FunctionDef) {
    this(TreeScalaConverter.getScalaAnonymousFunction(f, inputType))
  }

  def apply(in: TIn): TOut = {
    try {
      this.compiledFunction(in)
    }
    catch {
      case ex: Exception =>
        throw new RuntimeCompiledFunctionException("An error occurred in a runtime-compiled function.", this.functionDef, ex)
    }
  }

  def compileFunction(): TIn => TOut = {
    RuntimeEvaluator.instance.eval[TIn => TOut](this.functionDef)
  }
}


class RuntimeCompiledFunction2[T1, T2, +TOut](functionDef: String)
  extends SerializableFunction2[T1, T2, TOut] {

  @transient private lazy val compiledFunction = this.compileFunction()

  def this(inputType1: TypeDescriptor[_], inputType2: TypeDescriptor[_], f: FunctionDef) {
    this(TreeScalaConverter.getScalaAnonymousFunction(f, List(inputType1, inputType2)))
  }

  def apply(in1: T1, in2: T2): TOut = {
    try {
      this.compiledFunction(in1, in2)
    }
    catch {
      case ex: Exception =>
        throw new RuntimeCompiledFunctionException("An error occurred in a runtime-compiled function.", this.functionDef, ex)
    }
  }

  def compileFunction(): (T1, T2) => TOut = {
    RuntimeEvaluator.instance.eval[(T1, T2) => TOut](this.functionDef)
  }
}


class RuntimeCompiledOptionTuple1Function[TIn, T1](inputType: TypeDescriptor[_],
                                                   f: FunctionDef)
  extends SerializableFunction[TIn, Tuple1[Option[T1]]] {

  def this(inputType: TypeDescriptor[_], functions: List[FunctionDef]) {
    this(inputType, functions(0))
  }

  private val compiledFunction = new RuntimeCompiledFunction[TIn, T1](this.inputType, this.f)

  def apply(in: TIn): Tuple1[Option[T1]] = Tuple1(Some(this.compiledFunction(in)))
}


class RuntimeCompiledOptionTuple2Function[TIn, T1, T2](inputType: TypeDescriptor[_],
                                                       f1: FunctionDef,
                                                       f2: FunctionDef)
  extends SerializableFunction[TIn, (Option[T1], Option[T2])] {

  def this(inputType: TypeDescriptor[_], functions: List[FunctionDef]) {
    this(inputType, functions(0), functions(1))
  }

  private val compiledFunction1 = new RuntimeCompiledFunction[TIn, T1](this.inputType, this.f1)
  private val compiledFunction2 = new RuntimeCompiledFunction[TIn, T2](this.inputType, this.f2)

  override def apply(in: TIn): (Option[T1], Option[T2]) = (
    Some(this.compiledFunction1(in)),
    Some(this.compiledFunction2(in)))
}


class RuntimeCompiledOptionTuple3Function[TIn, T1, T2, T3](inputType: TypeDescriptor[_],
                                                           f1: FunctionDef,
                                                           f2: FunctionDef,
                                                           f3: FunctionDef)
  extends SerializableFunction[TIn, (Option[T1], Option[T2], Option[T3])] {

  def this(inputType: TypeDescriptor[_], functions: List[FunctionDef]) {
    this(inputType, functions(0), functions(1), functions(2))
  }

  private val compiledFunction1 = new RuntimeCompiledFunction[TIn, T1](this.inputType, this.f1)
  private val compiledFunction2 = new RuntimeCompiledFunction[TIn, T2](this.inputType, this.f2)
  private val compiledFunction3 = new RuntimeCompiledFunction[TIn, T3](this.inputType, this.f3)

  override def apply(in: TIn): (Option[T1], Option[T2], Option[T3]) = (
    Some(this.compiledFunction1(in)),
    Some(this.compiledFunction2(in)),
    Some(this.compiledFunction3(in)))
}


object RuntimeCompiledFunction {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  /**
   * Creates a [[RuntimeCompiledFunction]] for a function by converting it into a Milan function definition.
   *
   * @param f A function.
   * @tparam TIn  The input type of the function.
   * @tparam TOut The output type of the function.
   * @return A [[RuntimeCompiledFunction]] for the function.
   */
  def create[TIn, TOut](f: TIn => TOut): RuntimeCompiledFunction[TIn, TOut] = macro RuntimeCompiledFunctionMacros.create[TIn, TOut]

  /**
   * Creates a [[RuntimeCompiledFunction2]] for a function by converting it into a Milan function definition.
   *
   * @param f A function.
   * @tparam T1   The type of the first argument of the function.
   * @tparam T2   The type of the second argument of the function.
   * @tparam TOut The output type of the function.
   * @return A [[RuntimeCompiledFunction]] for the function.
   */
  def create[T1, T2, TOut](f: (T1, T2) => TOut): RuntimeCompiledFunction2[T1, T2, TOut] = macro RuntimeCompiledFunctionMacros.create2[T1, T2, TOut]

  /**
   * Creates a [[SerializableFunction]] that produces a tuple of options of the results of a set of element functions.
   *
   * @param inputType        The type of the input argument to the element functions.
   * @param inputTypeName    The name of the type of the input argument to the element functions.
   * @param elementFunctions Function definitions for the functions that compute the tuple elements.
   * @tparam TIn  The element function input argument type.
   * @tparam TOut The output tuple type.
   * @return A [[SerializableFunction]] that takes an input type instance and returns an output tuple type instance.
   */
  def createRuntimeCompiledOptionTupleFunction[TIn, TOut <: Product](inputType: TypeDescriptor[_],
                                                                     inputTypeName: String,
                                                                     elementFunctions: List[FunctionDef]): SerializableFunction[TIn, TOut] = {
    val elementTypeNames = elementFunctions.map(_.tpe.getTypeName)
    val elementTypeNamesList = elementTypeNames.mkString(", ")
    val functionTypeName = s"com.amazon.milan.flink.compiler.internal.RuntimeCompiledOptionTuple${elementFunctions.length}Function[$inputTypeName, $elementTypeNamesList]"

    this.logger.info(s"Creating instance of '$functionTypeName'.")

    val eval = RuntimeEvaluator.instance
    eval.evalFunction[TypeDescriptor[_], List[FunctionDef], SerializableFunction[TIn, TOut]](
      "inputType",
      TypeDescriptor.typeName("_"),
      "functions",
      s"List[${eval.getClassName[FunctionDef]}]",
      s"new $functionTypeName(inputType, functions)",
      inputType,
      elementFunctions)
  }
}


class RuntimeCompiledFunctionMacros(val c: whitebox.Context) extends ConvertExpressionHost {

  import c.universe._

  def create[TIn: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[TIn => TOut]): c.Expr[RuntimeCompiledFunction[TIn, TOut]] = {
    val functionDef = this.getMilanFunction(f.tree)
    val inputType = this.createTypeInfo[TIn]

    val tree = q"new ${weakTypeOf[RuntimeCompiledFunction[TIn, TOut]]}(${inputType.toTypeDescriptor}, $functionDef)"
    c.Expr[RuntimeCompiledFunction[TIn, TOut]](tree)
  }

  def create2[T1: c.WeakTypeTag, T2: c.WeakTypeTag, TOut: c.WeakTypeTag](f: c.Expr[(T1, T2) => TOut]): c.Expr[RuntimeCompiledFunction2[T1, T2, TOut]] = {
    val functionDef = this.getMilanFunction(f.tree)
    val input1Type = this.createTypeInfo[T1]
    val input2Type = this.createTypeInfo[T2]

    val tree = q"new ${weakTypeOf[RuntimeCompiledFunction2[T1, T2, TOut]]}(${input1Type.toTypeDescriptor}, ${input2Type.toTypeDescriptor}, $functionDef)"
    c.Expr[RuntimeCompiledFunction2[T1, T2, TOut]](tree)
  }
}
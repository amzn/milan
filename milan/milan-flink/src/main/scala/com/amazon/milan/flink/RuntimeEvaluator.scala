package com.amazon.milan.flink

import com.amazon.milan.flink.compiler.FlinkCompilationException
import com.amazon.milan.flink.components.{ParameterizedTypeInfo, TupleStreamTypeInformation}
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe
import scala.reflect.{ClassTag, classTag}
import scala.tools.reflect.ToolBox


class RuntimeEvaluationException(message: String, cause: Throwable) extends Exception(message, cause)

object RuntimeEvaluator {
  val default: RuntimeEvaluator = new RuntimeEvaluator()

  var instance: RuntimeEvaluator = this.default
}


class RuntimeEvaluator(classLoader: ClassLoader) {
  private val mirror = universe.runtimeMirror(this.classLoader)
  private val toolbox = ToolBox(this.mirror).mkToolBox()
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  def this() {
    this(getClass.getClassLoader)
  }

  /**
   * Evaluates scala code and returns the result.
   *
   * @param code The code to evaluate.
   * @tparam T The result type of the evaluation.
   * @return The result of the evaluation.
   */
  def eval[T](code: String): T = {
    this.logger.info(s"Evaluating code: $code")

    try {
      val tree = this.toolbox.parse(code)
      this.toolbox.eval(tree).asInstanceOf[T]
    }
    catch {
      case ex: Throwable =>
        throw new RuntimeEvaluationException(s"Error trying to evaluate code:\n$code", ex)
    }
  }

  /**
   * Compiles a function and then executes it and returns the result.
   *
   * @param body The code of the function body.
   * @tparam TOut The return type of the function.
   * @return The output of the function execution.
   */
  def evalFunction[TOut](body: String): TOut = {
    this.createFunction[TOut](body)()
  }

  /**
   * Compiles a function that takes one argument, and then calls it with the supplied argument and returns the result.
   *
   * @param body    The code of the function body.
   * @param argName The name of the function argument, as it is referenced in the function body.
   * @param argType The type of the function argument.
   * @param arg     The function argument.
   * @tparam TOut The return type of the function.
   * @return The output of the function execution.
   */
  def evalFunction[TIn, TOut](argName: String, argType: String, body: String, arg: TIn): TOut = {
    this.createFunction[TIn, TOut](argName, argType, body)(arg)
  }

  def evalFunction[T1, T2, TOut](arg1Name: String,
                                 arg1Type: String,
                                 arg2Name: String,
                                 arg2Type: String,
                                 body: String,
                                 arg1: T1,
                                 arg2: T2): TOut = {
    this.createFunction[T1, T2, TOut](arg1Name, arg1Type, arg2Name, arg2Type, body)(arg1, arg2)
  }

  def evalFunction[T1, T2, T3, TOut](arg1Name: String,
                                     arg1Type: String,
                                     arg2Name: String,
                                     arg2Type: String,
                                     arg3Name: String,
                                     arg3Type: String,
                                     body: String,
                                     arg1: T1,
                                     arg2: T2,
                                     arg3: T3): TOut = {
    val fun = this.createFunction[T1, T2, T3, TOut](arg1Name, arg1Type, arg2Name, arg2Type, arg3Name, arg3Type, body)
    fun(arg1, arg2, arg3)
  }

  def evalFunction[T1, T2, T3, T4, TOut](arg1Name: String,
                                         arg1Type: String,
                                         arg2Name: String,
                                         arg2Type: String,
                                         arg3Name: String,
                                         arg3Type: String,
                                         arg4Name: String,
                                         arg4Type: String,
                                         body: String,
                                         arg1: T1,
                                         arg2: T2,
                                         arg3: T3,
                                         arg4: T4): TOut = {
    val fun = this.createFunction[T1, T2, T3, T4, TOut](arg1Name, arg1Type, arg2Name, arg2Type, arg3Name, arg3Type, arg4Name, arg4Type, body)
    fun(arg1, arg2, arg3, arg4)
  }

  def evalFunction[T1, T2, T3, T4, T5, TOut](arg1Name: String,
                                             arg1Type: String,
                                             arg2Name: String,
                                             arg2Type: String,
                                             arg3Name: String,
                                             arg3Type: String,
                                             arg4Name: String,
                                             arg4Type: String,
                                             arg5Name: String,
                                             arg5Type: String,
                                             body: String,
                                             arg1: T1,
                                             arg2: T2,
                                             arg3: T3,
                                             arg4: T4,
                                             arg5: T5): TOut = {
    val fun = this.createFunction[T1, T2, T3, T4, T5, TOut](arg1Name, arg1Type, arg2Name, arg2Type, arg3Name, arg3Type, arg4Name, arg4Type, arg5Name, arg5Type, body)
    fun(arg1, arg2, arg3, arg4, arg5)
  }

  def evalFunction[T1, T2, T3, T4, T5, T6, TOut](arg1Name: String,
                                                 arg1Type: String,
                                                 arg2Name: String,
                                                 arg2Type: String,
                                                 arg3Name: String,
                                                 arg3Type: String,
                                                 arg4Name: String,
                                                 arg4Type: String,
                                                 arg5Name: String,
                                                 arg5Type: String,
                                                 arg6Name: String,
                                                 arg6Type: String,
                                                 body: String,
                                                 arg1: T1,
                                                 arg2: T2,
                                                 arg3: T3,
                                                 arg4: T4,
                                                 arg5: T5,
                                                 arg6: T6): TOut = {
    val fun = this.createFunction[T1, T2, T3, T4, T5, T6, TOut](arg1Name, arg1Type, arg2Name, arg2Type, arg3Name, arg3Type, arg4Name, arg4Type, arg5Name, arg5Type, arg6Name, arg6Type, body)
    fun(arg1, arg2, arg3, arg4, arg5, arg6)
  }

  def evalFunction[T1, T2, T3, T4, T5, T6, T7, TOut](arg1Name: String,
                                                     arg1Type: String,
                                                     arg2Name: String,
                                                     arg2Type: String,
                                                     arg3Name: String,
                                                     arg3Type: String,
                                                     arg4Name: String,
                                                     arg4Type: String,
                                                     arg5Name: String,
                                                     arg5Type: String,
                                                     arg6Name: String,
                                                     arg6Type: String,
                                                     arg7Name: String,
                                                     arg7Type: String,
                                                     body: String,
                                                     arg1: T1,
                                                     arg2: T2,
                                                     arg3: T3,
                                                     arg4: T4,
                                                     arg5: T5,
                                                     arg6: T6,
                                                     arg7: T7): TOut = {
    val fun = this.createFunction[T1, T2, T3, T4, T5, T6, T7, TOut](arg1Name, arg1Type, arg2Name, arg2Type, arg3Name, arg3Type, arg4Name, arg4Type, arg5Name, arg5Type, arg6Name, arg6Type, arg7Name, arg7Type, body)
    fun(arg1, arg2, arg3, arg4, arg5, arg6, arg7)
  }

  /**
   * Compiles a function that takes no arguments.
   *
   * @param body The function body.
   * @tparam TOut The return type of the function.
   * @return The compiled function.
   */
  def createFunction[TOut](body: String): () => TOut = {
    val code = s"() => { $body }"
    eval[() => TOut](code)
  }

  def createFunction[TIn, TOut](argName: String, argType: String, body: String): TIn => TOut = {
    val code = s"($argName: $argType) => { $body }"
    eval[TIn => TOut](code)
  }

  def createFunction[T1, T2, TOut](arg1Name: String,
                                   arg1Type: String,
                                   arg2Name: String,
                                   arg2Type: String,
                                   body: String): (T1, T2) => TOut = {
    val code = s"($arg1Name: $arg1Type, $arg2Name: $arg2Type) => { $body }"
    eval[(T1, T2) => TOut](code)
  }

  def createFunction[T1, T2, T3, TOut](arg1Name: String,
                                       arg1Type: String,
                                       arg2Name: String,
                                       arg2Type: String,
                                       arg3Name: String,
                                       arg3Type: String,
                                       body: String): (T1, T2, T3) => TOut = {
    val code = s"($arg1Name: $arg1Type, $arg2Name: $arg2Type, $arg3Name: $arg3Type) => { $body }"
    eval[(T1, T2, T3) => TOut](code)
  }

  def createFunction[T1, T2, T3, T4, TOut](arg1Name: String,
                                           arg1Type: String,
                                           arg2Name: String,
                                           arg2Type: String,
                                           arg3Name: String,
                                           arg3Type: String,
                                           arg4Name: String,
                                           arg4Type: String,
                                           body: String): (T1, T2, T3, T4) => TOut = {
    val code = s"($arg1Name: $arg1Type, $arg2Name: $arg2Type, $arg3Name: $arg3Type, $arg4Name: $arg4Type) => { $body }"
    eval[(T1, T2, T3, T4) => TOut](code)
  }

  def createFunction[T1, T2, T3, T4, T5, TOut](arg1Name: String,
                                               arg1Type: String,
                                               arg2Name: String,
                                               arg2Type: String,
                                               arg3Name: String,
                                               arg3Type: String,
                                               arg4Name: String,
                                               arg4Type: String,
                                               arg5Name: String,
                                               arg5Type: String,
                                               body: String): (T1, T2, T3, T4, T5) => TOut = {
    val code = s"($arg1Name: $arg1Type, $arg2Name: $arg2Type, $arg3Name: $arg3Type, $arg4Name: $arg4Type, $arg5Name: $arg5Type) => { $body }"
    eval[(T1, T2, T3, T4, T5) => TOut](code)
  }

  def createFunction[T1, T2, T3, T4, T5, T6, TOut](arg1Name: String,
                                                   arg1Type: String,
                                                   arg2Name: String,
                                                   arg2Type: String,
                                                   arg3Name: String,
                                                   arg3Type: String,
                                                   arg4Name: String,
                                                   arg4Type: String,
                                                   arg5Name: String,
                                                   arg5Type: String,
                                                   arg6Name: String,
                                                   arg6Type: String,
                                                   body: String): (T1, T2, T3, T4, T5, T6) => TOut = {
    val code = s"($arg1Name: $arg1Type, $arg2Name: $arg2Type, $arg3Name: $arg3Type, $arg4Name: $arg4Type, $arg5Name: $arg5Type, $arg6Name: $arg6Type) => { $body }"
    eval[(T1, T2, T3, T4, T5, T6) => TOut](code)
  }

  def createFunction[T1, T2, T3, T4, T5, T6, T7, TOut](arg1Name: String,
                                                       arg1Type: String,
                                                       arg2Name: String,
                                                       arg2Type: String,
                                                       arg3Name: String,
                                                       arg3Type: String,
                                                       arg4Name: String,
                                                       arg4Type: String,
                                                       arg5Name: String,
                                                       arg5Type: String,
                                                       arg6Name: String,
                                                       arg6Type: String,
                                                       arg7Name: String, arg7Type: String,
                                                       body: String): (T1, T2, T3, T4, T5, T6, T7) => TOut = {
    val code = s"($arg1Name: $arg1Type, $arg2Name: $arg2Type, $arg3Name: $arg3Type, $arg4Name: $arg4Type, $arg5Name: $arg5Type, $arg6Name: $arg6Type, $arg7Name: $arg7Type) => { $body }"
    eval[(T1, T2, T3, T4, T5, T6, T7) => TOut](code)
  }

  def getClassName[T: ClassTag]: String = {
    this.getClassName(classTag[T].runtimeClass)
  }

  def getClassName(cls: Class[_]): String = {
    if (cls.isArray) {
      "Array[Any]"
    }
    else {
      cls.getTypeName
    }
  }

  def getCorrectClassName(className: String): String = {
    tryFindClass(className) match {
      case Some(c) => c.getName
      case None => className
    }
  }

  def createTypeInformation[T](typeDescriptor: TypeDescriptor[T]): TypeInformation[T] = {
    if (typeDescriptor.isTuple && typeDescriptor.fields.nonEmpty) {
      // This is a tuple type with named fields, which means it's a record type for a stream.
      // For this we use TupleStreamTypeInformation.s
      TupleStreamTypeInformation.createFromFields(typeDescriptor.fields).asInstanceOf[TypeInformation[T]]
    }
    else if (typeDescriptor.isTuple) {
      this.createTupleTypeInformation(typeDescriptor.genericArguments).asInstanceOf[TypeInformation[T]]
    }
    else {
      val baseTypeInfo = this.createTypeInformation(typeDescriptor.fullName).asInstanceOf[TypeInformation[T]]

      // createTypeInformation will not produce a TypeInformation that exposes the generic type parameters.
      // We need to create TypeInformation for the type parameters ourselves, and then create a TypeInformation that
      // exposes them.
      if (typeDescriptor.genericArguments.isEmpty) {
        baseTypeInfo
      }
      else {
        val typeParameters = typeDescriptor.genericArguments.map(ty => this.createTypeInformation(ty))
        new ParameterizedTypeInfo[T](baseTypeInfo, typeParameters)
      }
    }
  }

  def createTupleTypeInformation[T <: Tuple](elementTypes: List[TypeDescriptor[_]]): TypeInformation[T] = {
    val className = TypeUtil.getTupleClassName(elementTypes.length)
    val cls = findClass(className).asInstanceOf[Class[T]]
    val elementTypeInfo = elementTypes.map(typeDesc => this.createTypeInformation(typeDesc))
    new TupleTypeInfo[T](cls, elementTypeInfo: _*)
  }

  def createTypeInformation(className: String): TypeInformation[_] = {
    val correctTypeName = this.getCorrectClassName(className)

    if (correctTypeName.contains(FlinkTypeNames.tuple)) {
      throw new IllegalArgumentException("Tuple types created by runtime evaluation are not serializable.")
    }
    else if (correctTypeName.startsWith("Stream[")) {
      throw new IllegalArgumentException("Attempted to create TypeInformation for non-existant Stream type.")
    }

    val typeInformation = evalFunction[TypeInformation[_]](s"org.apache.flink.api.scala.createTypeInformation[$correctTypeName]")
    if (typeInformation.getClass.getName.contains("__wrapper")) {
      throw new FlinkCompilationException(s"Couldn't create a valid TypeInformation for '$className' (renamed to '$correctTypeName').")
    }

    typeInformation
  }

  def createWrappedTypeInformation(className: String): TypeInformation[Any] = {
    val correctTypeName = this.getCorrectClassName(className)
    evalFunction[TypeInformation[Any]](s"com.amazon.milan.flink.components.TypeInformationConverterWrapper.wrapWithAny(org.apache.flink.api.scala.createTypeInformation[$correctTypeName])")
  }

  def tryFindClass(fullName: String): Option[Class[_]] = {
    try {
      Some(this.findClass(fullName))
    }
    catch {
      case _: ClassNotFoundException =>
        None
    }
  }

  def findClass(fullName: String): Class[_] = {
    def convertLastDotToDollar(str: String): List[String] = {
      val lastDot = fullName.lastIndexOf('.')
      if (lastDot < 0) {
        List()
      }
      else {
        List(fullName.substring(0, lastDot) + "$" + fullName.substring(lastDot + 1))
      }
    }

    val variants = List(fullName, s"scala.$fullName", s"java.lang.$fullName") ++ convertLastDotToDollar(fullName)

    variants
      .map(name => {
        try {
          Some(this.classLoader.loadClass(name))
        }
        catch {
          case _: ClassNotFoundException =>
            None
        }
      })
      .find(c => c.nonEmpty) match {
      case Some(Some(c)) =>
        c

      case _ =>
        val allNames = variants.map(name => s"'$name'").mkString("[", ", ", "]")
        throw new ClassNotFoundException(s"Unable to find class '$fullName' using class loader '${this.classLoader}'. Tried: $allNames.")
    }
  }
}

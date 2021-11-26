package com.amazon.milan.cmd

import java.lang.reflect.{Field, Method}
import scala.collection.mutable


class ArgumentsBase {
  private val arguments = this.findArgumentsVariables()
  private val argumentsWithValues = new mutable.HashSet[NamedArgument]()
  private val parametersArguments = this.findParametersArguments()

  this.validateArgumentDefinitions()

  def parse(args: Array[String], allowUnknownArguments: Boolean = false): Unit = {
    this.setDefaultValues()

    var remainingArgs = args.toList
    while (remainingArgs.nonEmpty) {
      remainingArgs = this.processNextArgument(remainingArgs, allowUnknownArguments)
    }

    this.arguments.foreach(arg => {
      val (argDef, _) = arg
      if (argDef.Required() && !argumentsWithValues.contains(argDef)) {
        throw new Exception(s"${argDef.Name()} is required.")
      }
    })
  }

  private def setDefaultValues(): Unit = {
    this.arguments.foreach {
      case (argDef, field) =>
        if (argDef.DefaultValue() != "") {
          this.setArgument(argDef, field, argDef.DefaultValue())
        }
    }
  }

  private def processNextArgument(args: List[String], allowUnknownArguments: Boolean): List[String] = {
    val arg = args.head

    if (arg.startsWith("--")) {
      val name = arg.substring(2)
      val argInfo = this.arguments.find(_._1.Name() == name)

      argInfo match {
        case None if !allowUnknownArguments =>
          throw new IllegalArgumentException(arg)

        case None if allowUnknownArguments =>
          // If the next arg doesn't exist or is an argument identifier then this arg was a boolean flag so
          // return the tail of the list.
          if (args.tail.isEmpty || args.tail.head.startsWith("-")) {
            args.tail
          }
          else {
            // The next arg is the value for this arg so skip it.
            args.tail.tail
          }

        case Some((argDef, field)) =>
          processArgument(argDef, field, args.tail)
      }
    }
    else if (arg.startsWith("-") && this.arguments.exists(_._1.ShortName() == arg.substring(1))) {
      val name = arg.substring(1)
      val (argDef, field) = this.arguments.filter(_._1.ShortName() == name).head
      this.processArgument(argDef, field, args.tail)
    }
    else if (arg.startsWith("-") && this.parametersArguments.contains(arg.substring(1, 2))) {
      val name = arg.substring(1, 2)
      val getter = this.parametersArguments(name)
      val parts = arg.substring(2).split("=", 2)
      val paramMap = getter.invoke(this)
      this.addToListBuffer(paramMap, parts(0), parts(1))
      args.tail
    }
    else if (allowUnknownArguments) {
      // If the next arg doesn't exist or is an argument identifier then this arg was a boolean flag so
      // return the tail of the list.
      if (args.tail.isEmpty || args.tail.head.startsWith("-")) {
        args.tail
      }
      else {
        // The next arg is the value for this arg so skip it.
        args.tail.tail
      }

    }
    else {
      throw new IllegalArgumentException(arg)
    }
  }

  private def processArgument(argDef: NamedArgument, field: Field, remainingArgs: List[String]): List[String] = {
    if (argDef.IsFlag()) {
      this.setArgument(argDef, field, true.toString)
      remainingArgs
    }
    else {
      this.setArgument(argDef, field, remainingArgs.head)
      remainingArgs.tail
    }
  }

  private def setArgument(argDef: NamedArgument, field: Field, stringValue: String): Unit = {
    val parsedValue = field.getType match {
      case t if t == classOf[String] =>
        stringValue

      case t if t == classOf[Int] =>
        stringValue.toInt

      case t if t == classOf[Boolean] =>
        stringValue.toBoolean

      case _ =>
        throw new Exception(argDef.Name + " - argument type is not supported.")
    }

    val setMethodName = field.getName + "_$eq"
    val setMethod = this.getClass.getMethods.find(_.getName.startsWith(setMethodName)).get
    setMethod.invoke(this, parsedValue.asInstanceOf[AnyRef])

    this.argumentsWithValues.add(argDef)
  }

  private def addToListBuffer(listBuffer: Any, key: String, value: String): Unit = {
    val appendMethod = listBuffer.getClass.getMethods.find(m => m.getName == "append").get

    if (appendMethod == null) {
      throw new Exception(s"Parameters argument field has unsupported type ${listBuffer.getClass.getName}")
    }

    appendMethod.invoke(listBuffer, List((key, value)))
  }

  private def findArgumentsVariables(): Array[(NamedArgument, Field)] = {
    val argumentFields = this.getClass.getDeclaredFields.filter(this.isNamedArgument)
    argumentFields.map(f => (f.getAnnotation(classOf[NamedArgument]), f))
  }

  private def isNamedArgument(field: Field): Boolean = {
    field.isAnnotationPresent(classOf[NamedArgument])
  }

  private def findParametersArguments(): Map[String, Method] = {
    val paramFields = this.getClass.getDeclaredFields.filter(this.isParametersArgument)
    val paramArgs =
      paramFields
        .map(f => {
          val method = this.getClass.getDeclaredMethod(f.getName)
          val prefix = f.getAnnotation(classOf[ParametersArgument]).Prefix()
          prefix -> method
        })
        .toMap

    if (paramArgs.exists { case (prefix, _) => prefix.length != 1 }) {
      throw new Exception("Prefix for parameters arguments must be a single character.")
    }

    paramArgs
  }

  private def isParametersArgument(field: Field): Boolean = {
    field.isAnnotationPresent(classOf[ParametersArgument])
  }

  private def validateArgumentDefinitions(): Unit = {
    this.arguments.foreach(arg => {
      val (argDef, field) = arg
      if (!argDef.Required() && argDef.DefaultValue() == "" && field.getType != classOf[String]) {
        throw new Exception(s"Argument definition for ${argDef.Name()} is invalid: optional arguments must have a default value.")
      }
      else if (argDef.Required() && argDef.DefaultValue() != "") {
        throw new Exception(s"Argument definition for ${argDef.Name()} is invalid: required arguments must not have a default value.")
      }
    })
  }
}

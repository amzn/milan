package com.amazon.milan.flink.apps

import java.lang.reflect.Field

import scala.collection.mutable


class ArgumentsBase {
  private val arguments = this.findArgumentsVariables()
  private val argumentsWithValues = new mutable.HashSet[NamedArgument]()

  this.validateArgumentDefinitions()

  def parse(args: Array[String]): Unit = {
    this.setDefaultValues()

    var remainingArgs = args.toList
    while (remainingArgs.nonEmpty) {
      remainingArgs = this.processNextArgument(remainingArgs)
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

  private def processNextArgument(args: List[String]): List[String] = {
    val arg = args.head

    if (arg.startsWith("--")) {
      val name = arg.substring(2)
      val argInfo = this.arguments.find(_._1.Name() == name)

      argInfo match {
        case None => throw new IllegalArgumentException(arg)
        case Some((argDef, field)) => processArgument(argDef, field, args.tail)
      }
    }
    else if (arg.startsWith("-")) {
      val name = arg.substring(1)
      val argInfo = this.arguments.find(_._1.ShortName() == name)

      argInfo match {
        case None => throw new IllegalArgumentException(arg)
        case Some((argDef, field)) => processArgument(argDef, field, args.tail)
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

  private def setArgument(argDef: NamedArgument, field: Field, stringValue: String) = {
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

  private def findArgumentsVariables(): Array[(NamedArgument, Field)] = {
    val argumentFields = this.getClass.getDeclaredFields
      .filter(this.isNamedArgument)

    argumentFields.map(f => (f.getAnnotation(classOf[NamedArgument]), f))
  }

  private def isNamedArgument(field: Field): Boolean = {
    field.isAnnotationPresent(classOf[NamedArgument])
  }

  private def validateArgumentDefinitions(): Unit = {
    this.arguments.foreach(arg => {
      val (argDef, field) = arg
      if (!argDef.Required() && argDef.DefaultValue() == "") {
        throw new Exception(s"Argument definition for ${argDef.Name()} is invalid: optional arguments must have a default value.")
      }
      else if (argDef.Required() && argDef.DefaultValue() != "") {
        throw new Exception(s"Argument definition for ${argDef.Name()} is invalid: required arguments must not have a default value.")
      }
    })
  }
}

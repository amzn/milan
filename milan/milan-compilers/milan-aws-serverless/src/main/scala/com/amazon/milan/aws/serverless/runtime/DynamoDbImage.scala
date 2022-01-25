package com.amazon.milan.aws.serverless.runtime


object DynamoDbImage {
  def toJson(image: Map[String, MilanDynamoDbAttributeValue]): String = {
    val sb = new IndentingStringBuilder(new StringBuilder, "")

    this.writeObject(sb)(image)

    sb.toString()
  }

  private def writeJson(sb: IndentingStringBuilder)(value: MilanDynamoDbAttributeValue): Unit = {
    if (value.isNull) {
      sb.append("null")
    }
    else if (value.isBoolean) {
      sb.append(value.BOOL.get.toString)
    }
    else if (value.isString) {
      sb.append("\"" + value.S + "\"")
    }
    else if (value.isNumber) {
      sb.append(value.N)
    }
    else if (value.isNumberSet) {
      this.writeArray(sb, (innerSb, item: String) => innerSb.append(item))(value.NS)
    }
    else if (value.isStringSet) {
      this.writeArray(sb, (innerSb, item: String) => innerSb.append("\"" + item + "\""))(value.SS)
    }
    else if (value.isList) {
      this.writeArray(sb, (innerSb, item: MilanDynamoDbAttributeValue) => this.writeJson(innerSb)(item))(value.L)
    }
    else if (value.isMap) {
      this.writeObject(sb)(value.M)
    }
  }

  private def writeArray[T](sb: IndentingStringBuilder, formatter: (IndentingStringBuilder, T) => Unit)
                           (items: List[T]): Unit = {
    sb.endLine("[")

    val innerSb = sb.indent()
    var first = true
    items.foreach(item => {
      if (first) {
        first = false
      }
      else {
        innerSb.endLine(",")
      }
      innerSb.startLine()
      formatter(innerSb, item)
    })
    innerSb.endLine()
    sb.startLine("]")
  }

  private def writeObject(sb: IndentingStringBuilder)(fields: Map[String, MilanDynamoDbAttributeValue]): Unit = {
    sb.endLine("{")

    val innerSb = sb.indent()
    var first = true

    fields.foreach {
      case (name, value) =>
        if (first) {
          first = false
        }
        else {
          innerSb.endLine(",")
        }

        innerSb.startLine("\"" + name + "\"").append(": ")
        this.writeJson(innerSb)(value)
    }
    innerSb.endLine()
    sb.startLine("}")
  }

  class IndentingStringBuilder(sb: StringBuilder, indent: String) {
    def append(text: String): IndentingStringBuilder = {
      this.sb.append(text)
      this
    }

    def startLine(text: String = ""): IndentingStringBuilder = {
      this.sb.append(this.indent).append(text)
      this
    }

    def endLine(text: String = ""): IndentingStringBuilder = {
      this.sb.append(text).append("\n")
      this
    }

    def appendLine(line: String): IndentingStringBuilder = {
      this.sb.append(this.indent).append(line).append("\n")
      this
    }

    def indent(): IndentingStringBuilder =
      new IndentingStringBuilder(this.sb, this.indent + "  ")

    override def toString: String =
      this.sb.toString()
  }
}

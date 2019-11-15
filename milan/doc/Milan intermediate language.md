### Milan IL ###
Programs written using the Milan Scala DSL are not executed as-is when the Scala code is run.
Instead the Scala code produces a definition of the data-oriented application in an intermediate language, which can then be compiled
and executed on multiple platforms.

Milan programs are stored in an abstract syntax tree. The tree node types can be found in the com.amazon.milan.program package, in Expression.scala and StreamExpressions.scala.
Milan expressions fall into two broad categories: standard expressions and graph expressions.

Standard expressions operate on familiar data types and objects, and are the logical and arithmetic expressions used to construct functions.

Graph expressions define streams and their relationships.
Examples of graph expressions are GroupBy, LeftJoin, and TumblingWindow.
These expressions generally take one or more graph expressions as input and their output type is a graph object like a stream, joined stream, or grouped stream.

### Examples ###
The following are examples of Milan Scala code and the corresponding Milan IL.

---
Scala:
```
val a = Stream.of[IntRecord].withId("A")
val b = A.map(r => new IntRecord(r.value + 1))
```
Milan definition of `b`:
```
MapRecord(
  Ref("A"),
  FunctionDef(
    List("r"),
    CreateInstance(
      TypeDescriptor("my.package.IntRecord"),
      List(Plus(SelectField(SelectTerm("r"), "value"), ConstantValue(1, TypeDescriptor("Int"))))
    )
  )
)
```
---
Scala:
```
val left = Stream.of[KeyValueRecord].withId("Left")
val right = Stream.of[KeyValueRecord].withId("Right")
val joined = left.leftJoin(right).where((l, r) => l.key == r.key)
                 .select(((l: KeyValueRecord, r: KeyValueRecord) => l) as "left",
                         ((l: KeyValueRecord, r: KeyValueRecord) => r) as "right")
```
Milan definition of `joined`:
```
MapFields(
  LeftJoin(
    Ref("Left"),
    Ref("Right"),
    FunctionDef(List("l", "r"), Equals(SelectField(SelectTerm("l"), "key"), SelectField(SelectTerm("r"), "key"))
  ),
  List(
    FieldDefinition("left", FunctionDef(List("l", "r"), SelectTerm("l"))),
    FieldDefinition("right", FunctionDef(List("l", "r"), SelectTerm("r")))
  )
)
```

Data streams are the building blocks of data-oriented applications in Milan.
The Milan language differentiates between two types of streams: *record streams* and *tuple streams*.

*Record streams* are streams whose records are represented by a single user-defined type.
In order for Milan's automated lineage tracking to work, these record types must provide a recordId field.
In the Milan Scala DSL, record streams are represented by the `ObjectStream[T]` class.

*Tuple streams* are streams whose records contain one or more named fields.
The types of these fields can be any simple or complex type.
There is no requirement the fields of tuple streams provide a record ID, because the tuple record itself has a record ID that allows for lineage tracking.
In the Milan Scala DSL, tuple streams are represented by the `TupleStream[T]` class.

Both record and tuple streams support the same basic operations: `map`, `join`, `groupBy`, and the time windowing operations `tumblingWindow` and `slidingWindow`.
These operations are declarative in that they define a relationship betweens streams but do not specific the implementation of that relationship.
However, they are different from the declarative relationships we are familiar with from SQL because they have an implicit time-based component.

### Mappings ###
`map` declares a 1:1 mapping between records on two streams.

A `map` operation specifies a function (or functions) that take the input record type as an argument and compute some output value.
If the map function is a single function then the output will be a record stream.
Alternatively you can supply one or more functions with an associated field name (by applying the `as` statement to an anonymous function) to create a tuple stream.

`toField` is syntactic sugar for a map operation on a record stream that produces a tuple stream with a single field, containing the record objects from the input stream.

`addField` is syntactic sugar for a map operation on a tuple stream that adds a new field which is computed using the existing fields.

### Filters ###
`filter` declares a mapping between two streams where the output stream contains a subset of the input stream records.

A `filter` operation specifies a boolean function that, and only records where the function returns true are present on the output stream.

### Joins ###
Join operations declare a relationship between two input streams and an output stream.

#### Enrichment Joins ####
Both `leftJoin` and `fullJoin` are enrichment joins when used to connect two data streams.
This means that when a record arrives on one of the streams it is paired with the most recent record from the other stream that satisfies the equality constraints in the join condition.
Join conditions are specified as a boolean function that takes one record from each stream as arguments.
Join conditions can be a mixture of equality constraints between the streams, constraints on a single stream, and arbitrary constraints that apply across the streams.

For example, consider the join condition in `A.leftJoin(B).where((a, b) => a.value < 3 && a.key == b.key && (a.value > b.value + 1))`
This has one pre-conditions on stream A: the "value" field must be less than 3.
Records that fail the pre-condition are filtered before being considered as join candidates. 
It has one equality constraint, which is that the "key" fields are equal.
Whenever a record arrives on A or B, it is paired with the latest record from the other stream that has the same key, and any remaining constraints are then applied to this pair of records.

#### Outer and Inner Joins ####
Outer and inner joins are joins between a data stream and a windowed stream.
When a record arrives on one stream, the join condition is used to find all matching records from the records in the appropriate window.
These joins are not yet implemented in Milan.

#### Join Output ####
The `select` operation tells Milan how to produce output records when a pair of input records is found that satisfies a join condition. 
`select` is essentially the same as the `map` operation except that the functions supplied take two arguments rather than one.
Like `map`, `select` can produce a record stream or a tuple stream.
Note that unless a not-null constraint is applied to both streams in the join condition, one of the arguments to the `select` functions could be null, so they must handle this otherwise a runtime error will occur.

### Grouping ###
Grouping operations declare a relationship between groups of records on an input stream and records on an output data stream.

#### GroupBy ####
The `groupBy` operation assigns records to groups based on a keys extracted from records.
Aggregate operations can then be applied to the groups using a `select` statement, which like other `select` and `map` statements
can produce a record stream or a tuple stream.
The functions provided to the `select` statement take two arguments:
the first argument type is the type of the key used for group assignment, and the second argument type is the record type of the input stream.
In one of these functions, aggregate operations like `sum`, `mean`, and `argmin` can be applied to the record argument, and the result of those can be combined with the group key.

Note that because the data is an infinite stream, the aggregation is performed and output every time a new record arrives for a group.
The number of output records for a given group key will be the same as the number of input records.

#### Time Windows ####
Time windows (`slidingWindow` and `tumblingWindow`) are similar to `groupBy` but with some differences:
* In the case of `slidingWindow`, a given record can belong to more than one window.
* Time windows can be applied to data streams *or* grouped streams - a `slidingWindow` or
`tumblingWindow` can be applied to the result of a `groupBy` operation to produce time windows that are partitioned by the group key.
* Groups from `groupBy` exist forever, while time windows can eventually close and be discarded by the runtime to free up resources.

Time windows support the same aggregate operations that `groupBy` supports.
The group key argument to the `select` functions will be a time stamp that corresponds to the start of the window in question.

#### Uniqueness ####
In order to prevent double-counting, a uniqueness constraint can be applied to grouped or windowed streams before aggregating.
This is done using by using the `unique` operator.
`unique` takes a function that extracts a value from the input records, and guarantees that when the aggregation is performed
only the latest record for any given value will be included in the aggregate computation.

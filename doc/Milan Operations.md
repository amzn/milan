### Grouping
GroupBy produces a set of logical streams, one for each value of a key.
```
GroupBy(Stream[T], T => K) => GroupedStream[T, K]
```

### Time Windows
Milan has several time-windowing operations which logically are syntactic sugar for the following operation:
```
TimeWindow(Stream[T], T => Set[W]) => WindowedStream[T, W]
```
where W is the type of a window identifier, and all records that map to the same window identifier instance are placed in the same window.
The differences between TimeWindow and the GroupBy operation are:
1. Records can fall into multiple time windows, but only one group.
2. Assuming time moves (eventually) forward for the application, a given time window can eventually be considered "closed" and any resources associated with that window freed.
3. Time windows have an Apply operation that can apply a user function to the entire window contents, whereas groups can only be processed as streams.

Users of the language don't see the general `TimeWindow` construct above, instead they have two operations, one for constructing sliding windows and another for tumbling windows (which are just a special case of sliding windows).

Sliding windows are defined using:
```
SlidingWindow(Stream[T], T => Time, size: Duration, slide: Duration, offset: Duration) => WindowedStream[T, TimeStamp]
```
`T => TimeStamp` maps stream records to time stamp values so their window assignments can be computed.
The `size`, `slide`, and `offset` parameters define the windowing behavior: `size` is the length of a window, `slide` is the distance between windows, and `offset` allows one to shift windows in time.

### Joins

Milan has a number of different types of join operations.

```
LeftJoin(Stream[T], Stream[U], (T, U) => Boolean) => JoinedStream[T, U]
```
LeftJoin takes two streams and a join condition.
When a record arrives on the left stream - `Stream[T]` - is is paired with the most recent record from the right stream that satisfies the join condition.
These paired records are then available in the resulting joined stream, which can be further processed using a Map operation.

Caveat: in the current implementation, if a record arrives on the left stream and no matching record is found on the right stream, a joined record is produced with the right value as null.
This should probably be explicitly expressed as an Option in the output rather than using nulls.

```
FullJoin(Stream[T], Stream[U], (T, U) => Boolean) => JoinedStream[T, U]
```
FullJoin is like LeftJoin, except the logic is applied symmetrically when a record arrives on either stream.

Similar to LeftJoin, if no matching record is found on the other stream then the resulting joined record will have null for that value.

```
LeftInnerJoin(Stream[T], Stream[U], (T, U) => Boolean) => JoinedStream[T, U]
```
Like a LeftJoin except that records arriving on the left stream will wait for a matching record from the right stream before being passed to the output.

### Map
The Map operation on a Stream is straightforward:
```
Map(Stream[T], T => O) => Stream[O]
```

Map on a grouped stream applies a function to every grouping, where each grouping is represented as a tuple of a key and a stream of the records with that key.
Anything that can be done to a stream can be done inside the map function of a grouped stream, and the key can be referenced as a scalar value in the computation.
Note that the Map operation on a grouped stream preserves the group key.
```
Map(GroupedStream[T, K], (K, Stream[T]) => Stream[O]) => GroupedStream[O, K]```
```

Map on a JoinedStream is the *only* operation that can be performed on a JoinedStream.
It produces a regular Stream.
```
Map(JoinedStream[T, U], (T, U) => O) => Stream[O]
```

Map on a WindowedStream maps each window separately and maintains the window properties:
```
Map(WindowedStream[T, W], (W, Stream[T]) => Stream[O]) => WindowedStream[O, W]
```
Because the only type of windowed streams users can create are time windows, W will always be a TimeStamp type.
The value of W passed to the user's map function will be the start of the window.

### FlatMap
```
FlatMap(Stream[T], (T => Seq[O])) => Stream[O]
```
The standard definition of FlatMap would have the user supply a function `T => Stream[O]`, but there are no Milan operations that create a stream from a scalar (yet).
Instead we have the user supply a function that returns a sequence of records from user code.
This has the limitation that the Milan program can only be compiled to a platform that supports executing that user code.
This limitation will be removed if we introduce an Unfold operation to Milan.

FlatMap on a grouped stream applies the function to every grouping, and the subsequent streams are merged.
```
FlatMap(GroupedStream[T, K], (K, Stream[T]) => Stream[O]) => Stream[O]
```


FlatMap on a windowed stream operates the same as FlatMap on a grouped stream:
```
FlatMap(WindowedStream[T, W], (W, Stream[T]) => Stream[O]) => Stream[O]
```


### MaxBy, MinBy
```
MaxBy(Stream[T], T => A) => Stream[T]
```
The output stream contains the "largest" value on the input stream seen up to that point, where "largest" is defined using the natural ordering for an argument computed from each record.
In Milan, MaxBy and MinBy only produce an output record if the new record is larger or smaller than the previous one; this is in contrast to some streaming systems where a new output record is produced regardless of whether the input record changes the output.

### SumBy, ProductBy, MaxBy, MinBy, MeanBy, etc...
All of these idempotent operations follow the same basic pattern:
```
SumBy(Stream[T], T => A, (A, T) => O) => Stream[O]
```
The first function converts records into the values being summed.
The second function takes the current value of the running sum and the most recent input record to produce an output record.
The second function is necessary because a stream of raw numbers is not particularly useful without some context attached to them, so the user must be provided an opportunity to attach that context.

### Scan, Reduce

Scan and Reduce operations are also provided.

```
Scan(Stream[T], initialState: S, (S, T) => (S, Option[O])) => Stream[O]
```
Here S is the state type.
Unlike the common definition, Milan's scan has the user output an Option, and the resulting stream contains only the
records where a value is provided.
We could also make the programmer implement this by following their Scan with a Filter. 

```
Reduce(Stream[T], (T, T) => T) => Stream[T]
```

Many of the operations above could trivially be expressed as a Scan, but they are not expressed that way in the Milan AST.
This is because the Milan program should be understandable by a human, so encoding the intent of the programmer in a straightforward way is useful.
It's also helpful for the compiler to understand the intent, because many compilation targets support these higher-level operations so it's more efficient to use them rather than compile everything as a Scan operation.

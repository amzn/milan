## Milan

Milan is a data-oriented programming language and runtime infrastructure.

The Milan language is a DSL embedded in Scala. The output is an intermediate language that can be compiled to run on different target platforms. Currently there exists a single compiler that produces Flink applications.

The Milan runtime infrastructure compiles and runs Milan applications on a Flink cluster.

### Milan Language
The Milan language is similar in look and feel to other JVM-based streaming frameworks like Spark Streaming, Flink, or Kafka Streams. The main differences are that Milan uses higher-level constructs to build streaming applications, and that Milan applications are not tied to a specific runtime infrastructure.

Examples of the Milan language are available in [the samples](milan/milan-samples).

Some of the language features are described in [the docs](milan/doc).

### Runtime
Currently Milan has a single compiler and runtime, based on [Apache Flink](https://flink.apache.org).

There are three options for executing a Milan program:
1. In-process using Flink's mini-cluster. It is not necessary to have a Flink cluster running to use this mode, only that Flink libraries are present.
1. On a Flink cluster on the local machine.
1. Remotely on a Flink cluster that has the Milan Flink control plane running.

Examples of executing using these different modes are available in [the samples](milan/milan-samples).

## License

This project is licensed under the Apache-2.0 License.

## Milan

Milan is a data-oriented programming language and runtime infrastructure.

The Milan language is a DSL embedded in Scala.
The output is an intermediate language that can be compiled to run on different target platforms.

### Milan Language
The Milan language is similar in look and feel to other JVM-based streaming frameworks like Spark Streaming, Flink, or Kafka Streams. The main differences are that Milan uses higher-level constructs to build streaming applications, and that Milan applications are not tied to a specific runtime infrastructure.

Examples of the Milan language and how to build Milan applications using Maven and sbt are available in [the samples](milan/milan-samples/README.md).

Some of the language features are described in [the docs](doc).

### Compilers
There are several compilers available, in various states of maturity:
* The *scala stream compiler* produces a scala program that reads and outputs scala Streams.
* The *scala event compiler* produces a scala class that consumes individual records.
* The *Flink compiler* produces an [Apache Flink](https://flink.apache.org) application that can consume and write to several data sources.
* The *AWS serverless compiler* produces scala and cdk code for AWS Lambda functions.

Examples of how to integrate compilers into a Maven or sbt build are available in the [the samples](milan/milan-samples/README.md).

### Environment Setup
All dependent packages are pulled from Maven central, so no special setup should be needed.

If you want to use the AWS Serverless compiler, you will need to install the [AWS CDK](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwiN5qm30Mz1AhVKi1wKHf1yCBIQFnoECAkQAQ&url=https%3A%2F%2Faws.amazon.com%2Fcdk%2F&usg=AOvVaw2tPZlF03QH3o_EKwTkN7cO).

### Building
Please complete the steps in Environment Setup above before building for the first time.

`mvn clean package` will compile, run tests, and create jars of the Milan packages.

`mvn clean install` will compile, run tests, package, and install the snapshot version into your local maven repository.
You can then use these from other projects.

Build flags:
* `-Dnoflink` will prevent the Flink compiler and samples from building.
  The tests for these are time-consuming so if not using Flink it's better to skip them.
* `-DskipTests` will prevent tests from running when the `package` or `install` targets are used.
* `-Dnosamples` will skip building samples, which is useful if you just want the libs for use in your own projects.

## License

This project is licensed under the Apache-2.0 License.

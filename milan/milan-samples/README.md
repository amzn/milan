## Samples Overview

These samples show how to perform some basic data processing tasks using the Milan Scala DSL, and compile them into executable Flink applications using Maven.
There is also a sample of how to build a Milan application using sbt rather than Maven in [sbt](sbt/README.md).

Milan relies on code generation to produce executable applications, so there are two modules:
* `milan-sample-gen` defines the Milan applications.
* `milan-sample-exec` performs the code generation and builds the resulting generated applications.

The pom.xml file in `milan-sample-exec` demonstrates one way to invoke the code generation, using a Milan utility called `CompileApplicationInstance`.
`milan-sample-exec` invokes `CompileApplicationInstance` during the `generate-sources` phase of the Maven build to create scala files that are then compiled during the `compile` phase.

`CompileApplicationInstance` is part of `milan-tools` and relies on two interfaces, `ApplicationInstanceProvider` and `ApplicationInstanceCompiler`.
`ApplicationInstanceProvider` returns an `ApplicatonInstance` object which contain all of the information necessary to perform code generation: a Milan application and configuration.
`ApplicationInstanceProvider` must be implemented by the creator of a Milan application if they want to use Milan's build tools.
`ApplicationInstanceCompiler` is implemented by Milan code generators, such as Milan's Flink compiler.

## Building the samples

The sample jar will be built any time Maven's package phase is executed.
You can build the samples using `mvn package` from the root of the repo.
If you want to build *only* the samples you will first need to `mvn install` from the root of the repo so that the sample dependencies are available.

Building the samples produces a fat JAR in the `milan-sample-exec/target` folder which contains all of the dependencies necessary for running the samples.
There are two build profiles that can be used:
1. The default profile includes the Flink libraries in the sample JAR. Use this if you don't have a Flink cluster running on your machine.
2. The `no-package-flink` profile is triggered by including the argument `-Dpackageflink=false` when invoking `mvn package`. This prevents Flink JARs from being included in the fat JAR, which is necessary if you want to execute the samples on a Flink cluster. 

## Running the samples

Run the samples by executing the following commands in the target folder.

If you built using the default profile:
```
java -cp ./milan-sample-exec-0.8-SNAPSHOT.jar com.amazon.milan.samples.exec.GroupByFlatMap
```

If you built using the no-package-flink profile:
```
flink run -c com.amazon.milan.samples.exec.GroupByFlatMap ./milan-sample-exec-0.8-SNAPSHOT.jar
```

You can execute the other samples by replacing `com.amazon.milan.samples.exec.GroupByFlatMap` with the name of the sample class.
You can find the available samples in the [pom.xml](milan-sample-gen/pom.xml) in `milan-samples-gen`.

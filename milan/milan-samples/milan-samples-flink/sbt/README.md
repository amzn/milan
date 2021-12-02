This folder contains an example of building a Milan application using sbt.

Building using sbt requires installing the Milan libraries into your local maven repo by running `mvn install -DskipTests` from the repo root.

`sbt compile` will then build the application.

`build.sbt` and `project/plugins.sbt` contain documentation about how the Milan code generator is invoked.

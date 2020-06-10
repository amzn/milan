resolvers += Resolver.mavenLocal

// We need the Milan build tools available to sbt.
libraryDependencies += "com.amazon.milan" % "milan-tools" % "0.8-SNAPSHOT"

// We're using Milan's Flink compiler so we need to include milan-flink in the sbt dependencies.
libraryDependencies += "com.amazon.milan" % "milan-flink" % "0.8-SNAPSHOT"

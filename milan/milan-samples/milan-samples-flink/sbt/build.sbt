import java.nio.file._
import com.amazon.milan.tools._


// Define some common settings that are shared across projects.
// We need the manvenLocal resolver because the Milan libraries are in the local maven repo, assuming someone ran 'mvn install'.
lazy val common = Seq(
  scalaVersion := "2.12.10",
  organization := "com.example",
  version := "0.1",
  resolvers += Resolver.mavenLocal
)

// Define some Milan dependencies that are shared across projects.
val milanVersion = "0.8-SNAPSHOT"
val milanLang = "com.amazon.milan" % "milan-lang" % milanVersion
val milanFlink = "com.amazon.milan" % "milan-flink-compiler" % milanVersion
val milanTools = "com.amazon.milan" % "milan-tools" % milanVersion

// The app project defines our Milan applications.
lazy val app = project
  .settings(common)
  .settings(
    name := "example-app",
    libraryDependencies ++= Seq(milanLang, milanTools)
  )


// Define the tasks that will execute the code generation.
// We have one task per application, and another task that combines them all together.
lazy val generateFooApp = taskKey[Seq[File]]("Generate the Foo application.")
lazy val generateBarApp = taskKey[Seq[File]]("Generate the Bar application.")
lazy val generateAllApps = taskKey[Seq[File]]("Generate all applications.")

// This task defines where the generated Scala files are placed.
lazy val generatedCodeOutputPath = taskKey[Path]("Gets the path where generated code is placed.")

// We need the path where class files are generated from the app project so that we can add it to the classpath
// when running the code generation task.
lazy val appOutputClassRoot = taskKey[Path]("Gets the root output folder for .class files from the app project.")


// Creates a task that compiles a Milan application from the app project into a Flink application.
def compileFlinkApp(className: String): Def.Initialize[Task[Seq[File]]] = {
  Def.task {
    // This makes this task wait for the app project to finish compiling before executing.
    // If we don't do this, then we try to run the code generator before the provider classes from the app project
    // are available, and we get a ClassNotFound error.
    (app / Compile / compile).value

    // We have to add the output from the app project to the classpath at runtime, otherwise the Milan build tool
    // won't be able to load the class and get the application instance.
    addToSbtClasspath(Seq(appOutputClassRoot.value))

    // Call compileApplicationInstance which gets the application instance from our provider, sends it to the
    // Flink compiler, and writes the output to a file.
    // compileApplicationInstance returns File, which we wrap in a Seq so that sbt understands that we've produced
    // a managed code file.
    Seq(
      compileApplicationInstance(
        s"com.example.$className",
        List.empty,
        "flink",
        List(("package", "com.example"), ("class", s"Run$className")),
        generatedCodeOutputPath.value.resolve(s"Run$className.scala")))
  }
}


// The runner project is the target of the generated code, it will contain our compiled applications.
lazy val runner = project
  .dependsOn(app)
  .settings(common)
  .settings(
    name := "example-runner",
    libraryDependencies ++= Seq(milanTools, milanFlink),
  )
  .settings(
    generatedCodeOutputPath := Paths.get((Compile / sourceManaged).value.toString, "generated"),
    appOutputClassRoot := {
      ((app / Compile / target).value / "scala-2.12" / "classes").toPath
    },
    generateFooApp := compileFlinkApp("FooApp").value,
    generateBarApp := compileFlinkApp("BarApp").value,
    generateAllApps := {
      generateFooApp.value ++ generateBarApp.value
    },
    (sourceGenerators in Compile) += generateAllApps
  )


lazy val root = project
  .in(file("."))
  .settings(common)
  .settings(
    skip in publish := true
  )
  .aggregate(app, runner)

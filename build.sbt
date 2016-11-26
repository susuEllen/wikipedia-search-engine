name := "WikipediaSearchProject"

version := "1.0"

//scalaVersion := "2.12.0"
//scalaVersion := "2.11.8"
scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.2",
  "org.apache.spark" %% "spark-mllib" % "2.0.2"
)

// to use custom lib instead of default lib directory
//unmanagedBase := baseDirectory.value / "custom_lib"

fork := true
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "project4-topdev-spark"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0",
  "org.postgresql" % "postgresql" % "42.7.3"
)

assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)

assembly / mainClass := Some("topdev")

assembly / assemblyMergeStrategy := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

 assembly / assemblyJarName := "project4-topdev-fatjar-1.0.jar"

fork := true
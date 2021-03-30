name := "Twitch"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1"

libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.0.0"

libraryDependencies += "com.redislabs" %% "spark-redis" % "2.4.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

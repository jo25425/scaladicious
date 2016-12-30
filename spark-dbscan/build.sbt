name := "spark-dbscan"

version := "1.0"

//scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.0.1"
  Seq(
    "org.apache.spark"     %% "spark-core"              % sparkVer withSources(),
    "org.apache.spark"     %% "spark-mllib"             % sparkVer withSources(),
    "org.apache.spark"     %% "spark-sql"               % sparkVer withSources(),
    "org.apache.spark"     %% "spark-streaming"         % sparkVer withSources(),
//    "org.scalanlp"         % "nak"                     % "1.2.1"
    "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
  )
}
    
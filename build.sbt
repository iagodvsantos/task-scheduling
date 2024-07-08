lazy val akkaVersion = "2.8.6"
lazy val jclOverSlf4jVersion = "2.0.5"
lazy val logbackVersion = "1.5.6"
lazy val logstashVersion = "7.4"
lazy val postgresVersion = "42.7.3"
lazy val cassandraDriver = "4.18.1"

lazy val compileDependencies = Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "org.slf4j" % "jcl-over-slf4j" % jclOverSlf4jVersion,
  "net.logstash.logback" % "logstash-logback-encoder" % logstashVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.apache.cassandra" % "java-driver-core" % cassandraDriver
)

lazy val assemblySettings = Seq(
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _*) => MergeStrategy.first
    case PathList("META-INF", _*)             => MergeStrategy.discard
    case "reference.conf"                     => MergeStrategy.concat
    case _                                    => MergeStrategy.first
  }
)

lazy val root = (project in file("."))
  .settings(
    name := "task-scheduler",
    scalaVersion := "2.13.12",
    libraryDependencies ++= compileDependencies
  )
  .settings(assemblySettings)

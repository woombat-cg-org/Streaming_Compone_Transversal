
name := "Framework_json"

version := "8.4.7"
resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"


scalaVersion := "2.11.12"
val sparkversion = "2.4.0"

mainClass in Compile := Some("Framework_json")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkversion,
  "org.apache.spark" %% "spark-hive" % sparkversion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7" ,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" %  sparkversion ,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.1",
  "joda-time" % "joda-time" % "2.10.5",
  "org.apache.kudu" %% "kudu-spark2" % "1.5.0-cdh5.13.1",
  "org.apache.kudu" % "kudu-client" % "1.5.0-cdh5.13.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.1",
  ("org.apache.spark" %% "spark-core" % sparkversion).
  exclude("org.Eclipse.jetty.orbit", "javax.servlet").
  exclude("org.Eclipse.jetty.orbit", "javax.transaction").
  exclude("org.Eclipse.jetty.orbit", "javax.mail").
  exclude("org.Eclipse.jetty.orbit", "javax.activation").
  exclude("commons-beanutils", "commons-beanutils-core").
  exclude("commons-collections", "commons-collections").
  exclude("com.esotericsoftware.minlog", "minlog")
)

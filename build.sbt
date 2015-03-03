import AssemblyKeys._

name := "netflix-als"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.2.0"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.8.0"

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("META-INF", "ECLIPSEF.RSA" ) => MergeStrategy.discard
    case PathList("META-INF", "mailcap" ) => MergeStrategy.discard
    case PathList("com", "esotericsoftware", "minlog", ps ) if ps.startsWith("Log") => MergeStrategy.discard
    case PathList("plugin.properties" ) => MergeStrategy.discard
    case PathList("META-INF", ps @ _* ) => MergeStrategy.discard
    case PathList("javax", "activation", ps @ _* ) => MergeStrategy.first
    case PathList("org", "apache", "commons", ps @ _* ) => MergeStrategy.first
    case PathList("org", "apache", "hadoop", "yarn", ps @ _* ) => MergeStrategy.first
    case x => old(x)
  }
}


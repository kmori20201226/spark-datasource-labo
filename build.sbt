ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.6"

lazy val root = (project in file("."))
  .settings(
    name := "spark-datasource-labo",
    idePackagePrefix := Some("com.kmori20201226.spark_datasource.shapefile")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
  "org.geotools" % "gt-main" % "26.2",
  "org.geotools" % "gt-shapefile" % "26.2",
  "org.postgresql" % "postgresql" % "42.3.3",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.12.6"
)

resolvers += "OSGeo release" at "https://repo.osgeo.org/repository/release/"
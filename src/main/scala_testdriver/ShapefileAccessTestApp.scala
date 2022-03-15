package com.kmori20201226.testdriver

import org.geotools.data.{DataStoreFinder, Query}
import org.opengis.feature.Property
import org.opengis.filter.Filter

import java.io.File
import java.util.HashMap
import scala.jdk.CollectionConverters.CollectionHasAsScala

object ShapefileAccessTestApp {
  def readShapefile(pathName: String): Unit = {
    val file = new File(pathName)
    val map = new HashMap[String, Object]()
    map.put("url", file.toURI().toURL())
    //val map = Map("url"->file.toURI.toURL)

    val dataStore = DataStoreFinder.getDataStore(map)
    val typeName = dataStore.getTypeNames()(0)

    val source = dataStore.getFeatureSource(typeName)
    val filter = Filter.INCLUDE // ECQL.toFilter("BBOX(THE_GEOM, 10,20,30,40)")

    val collection = source.getFeatures(filter)
    try {
      val features = collection.features
      try {
        while (features.hasNext) {
          val feature = features.next
          System.out.print(feature.getID)
          System.out.print(": ")
          System.out.println(feature.getDefaultGeometryProperty.getValue)
        }
      }
      finally {
        if (features != null) features.close()
      }
    }
  }

  def readDbaseIVFile(pathName: String): Unit = {
    val file = new File(pathName)
    val map = new HashMap[String, Object]()
    map.put("url", file.toURI().toURL())
    //val map = Map("url"->file.toURI.toURL)

    val dataStore = DataStoreFinder.getDataStore(map)
    val typeName = dataStore.getTypeNames()(0)
    for (s <- dataStore.getTypeNames) {
      println("=" + s)
    }
    val source = dataStore.getFeatureSource(typeName)
    val schema = source.getSchema
    for (s <- schema.getAttributeDescriptors.asScala) {
      println(f"name=${s.getName} type=${s.getType} min=${s.getMinOccurs} max=${s.getMaxOccurs} user=${s.getUserData}")
    }
    println("-----------------------")
    for (s <- schema.getDescriptors.asScala) {
      println(f"name=${s.getName} type=${s.getType} min=${s.getMinOccurs} max=${s.getMaxOccurs} user=${s.getUserData}")
    }
    println(f"schema=${schema}")
    val query = new Query(schema.getTypeName)
    query.setMaxFeatures(1)

    val collection = source.getFeatures(query)
    try {
      val features = collection.features
      try {
        while (features.hasNext) {
          val feature = features.next
          System.out.println(feature.getID + ": ")
          for (attribute: Property <- feature.getProperties.asScala) {
            System.out.println("\t" + attribute.getName + ":" + attribute.getValue)
          }
        }
      }
      finally {
        if (features != null) features.close()
      }
    }
  }

  def main(argv: Array[String]): Unit = {
    readShapefile("../testdata/Z402843_ROAD_GENERAL1.shp")
    readDbaseIVFile("../testdata/Z402843_ROAD_GENERAL1.shp")
  }
}

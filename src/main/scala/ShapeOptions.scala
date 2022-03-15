package com.kmori20201226.datasource.shape

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

class ShapeOptions(options: Map[String, String]) extends Serializable {
  val parameters: CaseInsensitiveMap[String] = CaseInsensitiveMap(options)
}

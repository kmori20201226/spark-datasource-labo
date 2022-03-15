package com.kmori20201226.testdriver

import org.apache.parquet.format.IntType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object DatasourceAccessTestApp {
  def main(args: Array[String]): Unit = {
    _main()
  }
  private def _main(): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("datasource access test")
      .getOrCreate()
    if (true) {
      val schema = StructType(Array(
        StructField("key-a", IntegerType, true),
        StructField("key-b", ArrayType(ArrayType(IntegerType, false), false), true),
        StructField("key-c", StringType, true)
      ))
      val x = sparkSession.read
        // .schema(schema)
        .json("../testjson/data01.json")
      x.show()
    }
    if (false) {
      val simpleDf = sparkSession.read
        .option("header", true)
        .csv("../testdata/")
      simpleDf.show()
    }
    if (false) {
      val df = sparkSession.read.format("shape")
        .option("shapetype", "P_AREA_ADMINISTRATION")
        .load("../testdata/*.dbf")
      df.printSchema()
      df.show()
    }
  }
  }

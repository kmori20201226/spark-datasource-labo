package com.kmori20201226.datasource.shape

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVHeaderChecker, CSVOptions, UnivocityParser}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFileLinesReader, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class ShapeFileFormat extends FileFormat with DataSourceRegister {
  override def shortName(): String = "shape"

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = new ShapeOptions(options)
    ShapeDataSource.inferSchema(sparkSession, files, parsedOptions)
  }
  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val parsedOptions = new ShapeOptions(options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      ShapeDataSource.readFile(conf, file, requiredSchema, parsedOptions)
    }
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ".dbf"

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new OutputWriter {
          override def write(row: InternalRow): Unit = ???

          override def close(): Unit = ???

          override def path(): String = ???
        }
      }
    }
  }
}

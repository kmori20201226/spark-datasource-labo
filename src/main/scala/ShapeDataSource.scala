package com.kmori20201226.datasource.shape

import com.kmori20201226.datasource.shape.ShapeDataSource.isInterestedPath
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.execution.datasources.{DataSource, HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, IntegerType, LongType, Metadata, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.data.{DataStoreFinder, FileDataStore, FileDataStoreFinder}
import org.geotools.data.shapefile.dbf.DbaseFileReader
import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.shp.{ShapeType, ShapefileReader}
import org.locationtech.jts.awt.PointShapeFactory.X
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, MultiPolygon, Polygon}

import java.io.File
import java.nio.charset.Charset
import java.net.URI
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, OpenOption, Path, Paths}

abstract class ShapeDataSource extends Serializable {
  /**
   * Infers the schema from `inputPaths` files.
   */
  final def inferSchema(
                         sparkSession: SparkSession,
                         inputPaths: Seq[FileStatus],
                         parsedOptions: ShapeOptions): Option[StructType] = {
    if (inputPaths.nonEmpty) {
      Some(infer(sparkSession, inputPaths, parsedOptions))
    } else {
      None
    }
  }

  protected def infer(
                       sparkSession: SparkSession,
                       inputPaths: Seq[FileStatus],
                       parsedOptions: ShapeOptions): StructType

}

object ShapeDataSource extends ShapeDataSource with Logging {
  private def isInterestedPath(path: String, parsedOptions: ShapeOptions): Boolean = {
    val shapeType = parsedOptions.parameters.getOrElse("shapeType", null)
    val tail = if (shapeType == null) ".dbf" else s"_${shapeType}.dbf"
    path.endsWith(tail)
  }

  def newStruct(seq: String): DataType = {
    var ls : List[String] = List[String]()
    for (s <- seq) {
      s match {
        case 'd' => DoubleType
      }
    }
    new StructType()
  }

  private def pointType = {
    new StructType(Array(
      new StructField("point",
        new ArrayType(DoubleType, false),
        false, Metadata.empty)))
  }

  private def multipointType = {
    new StructType(Array(
      new StructField("points",
        new ArrayType(new ArrayType(DoubleType, false), false),
        false, Metadata.empty)))
  }

  private def multilineType = {
    // org.geotools.data.shapefile.shp.MultiLineString
    new StructType(Array(
      new StructField("lines",
        new ArrayType(
          new ArrayType(new ArrayType(DoubleType, false), false),
          false
        )
      )
    ))
  }

  private def polygonType = {
    new StructType(Array(
      new StructField("shell",
        new ArrayType(new ArrayType(DoubleType, false), false)),
      new StructField( "holes",
        new ArrayType(new ArrayType(new ArrayType(DoubleType, false), false), false),
      )
    ))
  }

  private def multipolygonType = {
    new ArrayType(polygonType, false)
  }

  def getShapeType(shapeType: ShapeType): DataType = {
    shapeType match {
      case ShapeType.NULL => null
      case ShapeType.POINT => pointType
      case ShapeType.POINTZ => pointType
      case ShapeType.POINTM => pointType
      case ShapeType.ARC => multilineType
      case ShapeType.ARCZ => multilineType
      case ShapeType.ARCM => multilineType
      case ShapeType.MULTIPOINT => multipointType
      case ShapeType.MULTIPOINTZ => multipointType
      case ShapeType.MULTIPOINTM =>multipointType
      case ShapeType.POLYGON => multipolygonType
      case ShapeType.POLYGONZ => multipolygonType
      case ShapeType.POLYGONM => multipolygonType
      case _ => null
    }
  }

  override def infer(
                      sparkSession: SparkSession,
                      inputPaths: Seq[FileStatus],
                      parsedOptions: ShapeOptions): StructType = {

    val paths = inputPaths.map(_.getPath.toString).filter(isInterestedPath(_, parsedOptions))
    if (paths.length == 0) {
      throw new Exception("No dbf file found")
    }
    var s = new StructType
    var used = Set[String]()
    for (path <- paths) {
      val dbf_uri = new URI(path)
      val dbf_chan = FileChannel.open(Paths.get(dbf_uri.getPath))
      val shp_uri = new URI(path.substring(0, path.lastIndexOf('.')) + ".shp")
      val st = if ((new File(shp_uri.getPath)).exists) {
        val shpReader = new ShapefileReader(new ShpFiles(shp_uri.getPath), true, false,
          new GeometryFactory(), false)
        try
        {
          getShapeType(shpReader.getHeader.getShapeType)
        } finally {
          shpReader.close()
        }
      } else null
      try {
        val dbfReader = new DbaseFileReader(dbf_chan, false, Charset.forName("ISO-8859-1"))
        val header = dbfReader.getHeader()
        for (i <- 0 until header.getNumFields) {
          val fieldName = header.getFieldName(i)
          if (!used.contains(fieldName)) {
            used = used + fieldName
            val dataType: DataType = header.getFieldType(i) match {
              case 'C' => StringType
              case 'N' => if (header.getFieldDecimalCount(i) == 0) LongType else DoubleType
              case 'F' => DoubleType
              case 'L' => StringType
              case _ => StringType
            }
            val md = new MetadataBuilder()
              .putLong("fieldLength", header.getFieldLength(i))
              .putLong("decimalCount", header.getFieldDecimalCount(i))
              .putLong("fieldType", header.getFieldType(i))
            s = s.add(header.getFieldName(i), dataType, true, md.build())
          }
        }
        if (st != null) {
          if (!used.contains("__geometry__")) {
            used = used + "__geometry__"
            s = s.add("__geometry__", st, false, Metadata.empty)
          }
        }
      } finally {
        dbf_chan.close()
      }
    }
    return s
  }

  def readFile(
               conf: Configuration,
               file: PartitionedFile,
               requiredSchema: StructType,
               parsedOptions: ShapeOptions): Iterator[InternalRow] = {
    if (isInterestedPath(file.filePath, parsedOptions)) {
      new DbfIterator(file, requiredSchema)
    } else {
      new Iterator[InternalRow] {
        override def hasNext: Boolean = false
        override def next(): InternalRow = ???
      }
    }
  }
}

class DbfIterator(file: PartitionedFile, requiredSchema: StructType) extends Iterator[InternalRow] {
  val uri = new URI(file.filePath)
  val fc = FileChannel.open(Paths.get(uri.getPath))
  val shpFilePath = uri.getPath.substring(0, uri.getPath.lastIndexOf('.')) + ".shp"
  val shpReader = if (new File(shpFilePath).exists) {
    new ShapefileReader(new ShpFiles(shpFilePath), true, false,
      new GeometryFactory(), false)
  } else null
  val dbfReader = new DbaseFileReader(fc, false, Charset.forName("ISO-8859-1"))
  val header = dbfReader.getHeader()
  val targetFieldIndices = Array.fill[Option[Int]](header.getNumFields)(null)
  val targetFieldTypes = Array.fill[Option[DataType]](header.getNumFields)(null)
  for (i <- 0 until header.getNumFields) {
    try {
      val dx = requiredSchema.fieldIndex(header.getFieldName(i))
      targetFieldIndices(i) = Option(dx)
      targetFieldTypes(i) = Option(requiredSchema.fields(dx).dataType)
    } catch {
      case _: java.lang.IllegalArgumentException =>
        targetFieldIndices(i) = null
        targetFieldTypes(i) = null
    }
  }

  def toArray(c: Coordinate, dim: Int): GenericArrayData = {
    dim match {
      case 2 => new GenericArrayData(Array(c.getOrdinate(0), c.getOrdinate(1)))
      case 3 => new GenericArrayData(Array(c.getOrdinate(0), c.getOrdinate(1), c.getOrdinate(2)))
      case _ => throw new IllegalArgumentException("unknown multipoly dimension (" + dim + ")")
    }
  }
  private def expandPolygon(poly: Polygon, dim: Int): InternalRow = {
    val shell = poly.getExteriorRing.getCoordinates.map[GenericArrayData](p => toArray(p, dim)).toArray
    val holes = scala.collection.mutable.ListBuffer.empty[Array[GenericArrayData]]
    for (i <- 0 until poly.getNumInteriorRing) {
      holes += poly.getInteriorRingN(i).getCoordinates.map[GenericArrayData](p => toArray(p, dim)).toArray
    }
    val ir = new GenericInternalRow(2)
    ir.update(0, new GenericArrayData(shell))
    ir.update(1, new GenericArrayData(holes))
    ir
  }
  private def expandMultiPolygon(multiPoly: MultiPolygon): GenericArrayData = {
    val l = scala.collection.mutable.ListBuffer.empty[InternalRow]
    for (n <- 0 until multiPoly.getNumGeometries) {
      multiPoly.getGeometryN(n) match {
        case poly: Polygon =>
          l += expandPolygon(poly, multiPoly.getDimension)
      }
    }
    new GenericArrayData(l.result.toArray)
  }

  override def hasNext: Boolean = dbfReader.hasNext

  override def next(): InternalRow = {
    val resultRow = new GenericInternalRow(requiredSchema.length)
    if (shpReader != null) {
      val shpRec = shpReader.nextRecord
      try {
        val dx = requiredSchema.fieldIndex("__geometry__")
        val v = shpRec.getSimplifiedShape()
        shpRec.getSimplifiedShape match {
          case v: MultiPolygon =>
            val mp = expandMultiPolygon(v)
            resultRow.update(dx, mp)
          case v => resultRow.update(dx, v.toString)
        }
      } catch {
        case _: java.lang.IllegalArgumentException => ;
      }
    }
    val entries = dbfReader.readEntry()
    for (i <- 0 until entries.length) {
      targetFieldIndices(i) match {
        case Some(dx) =>
          val sv = entries(i)
          targetFieldTypes(i).get match {
            case IntegerType =>
              sv match {
                case o: Integer => resultRow.setInt(dx, o.intValue)
                case o: java.lang.Long => resultRow.setInt(dx, o.intValue)
                case null => ;
              }
            case LongType =>
              sv match {
                case o: Integer => resultRow.setLong(dx, o.longValue)
                case o: java.lang.Long => resultRow.setLong(dx, o.longValue)
                case null => ;
              }
            case StringType =>
              sv match {
                case null =>;
                case _ => resultRow.update(dx, UTF8String.fromString(sv.toString))
              }
            case FloatType =>
              sv match {
                case o: java.lang.Float => resultRow.setFloat(dx, o.floatValue)
                case o: java.lang.Double => resultRow.setFloat(dx, o.floatValue)
                case null => ;
              }
            case DoubleType =>
              sv match {
                case o: java.lang.Float => resultRow.setDouble(dx, o.doubleValue)
                case o: java.lang.Double => resultRow.setDouble(dx, o.doubleValue)
                case null => ;
              }
            case AnyRef =>
              sv match {
                case null => ;
                case _ => resultRow.update(dx, UTF8String.fromString(sv.toString))
              }
            case _ => ;
          }
        case _ => ;
      }
    }
    resultRow
  }
}
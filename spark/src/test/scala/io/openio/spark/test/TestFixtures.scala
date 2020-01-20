package io.openio.spark.test

import java.io.{File, IOException}

import io.openio.spark.utils.SparkUtils.writeOutput
import io.openio.spark.workload.KMeansGenWorkload
import org.apache.spark.mllib.util.KMeansDataGenerator
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class TestFixtures(path: String) {
  val testFolderPath = "/tmp/hadoop-bench-test/" + path

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  def createWorkspace(): Unit = {
    val testFolder = new File(testFolderPath)
    testFolder.mkdirs()
  }

  def deleteWorkspace(): Unit = {
    val testFolder = new File(testFolderPath)
    deleteRecursively(testFolder)
  }

  def listFiles(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException(s"Failed to list files for dir: $file")
      }
      files
    } else {
      List()
    }
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      for (child <- listFiles(file)) {
        deleteRecursively(child)
      }
    }
    file.delete()
  }

  def generateData(numRows: Int, numCols: Int, output: String): Unit = {
    val data = KMeansDataGenerator.generateKMeansRDD(
      spark.sparkContext,
      numRows,
      KMeansGenWorkload.numClusters,
      numCols,
      KMeansGenWorkload.scaling,
      KMeansGenWorkload.numPartitions
    )
    val schemaString = data.first().indices.map(i => "col_" + i.toString).mkString(" ")
    val fields = schemaString.split(" ")
      .map(n => StructField(n, DoubleType, nullable = false))
    val schema = StructType(fields)
    val rowRDD = data.map(arr => Row(arr: _*))
    val df = spark.createDataFrame(rowRDD, schema)
    writeOutput(output, df, spark)
  }
}

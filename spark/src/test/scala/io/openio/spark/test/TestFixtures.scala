package io.openio.spark.test

import java.io.File

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
    // TODO cleanup testFolder
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

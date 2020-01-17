package io.openio.spark.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {
  def checkPathExistsOrThrow(path: String, spark: SparkSession): Unit = {
    if (!checkPathExists(path, spark)) {
      throw new Exception(s"Path does exists: $path")
    }
  }

  def checkPathExists(path: String, spark: SparkSession): Boolean = {
    val hadoopPath = new Path(path)
    hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(hadoopPath)
  }

  def loadInput(input: String, spark: SparkSession): DataFrame = {
    checkPathExists(input, spark)

    val format = parseFormat(input)
    format match {
      case Formats.parquet => spark.read.parquet(input)
      case Formats.csv => spark.read.csv(input)
      case _ => throw new Exception(s"Unknown load format: $format")
    }
  }

  def writeOutput(output: String, df: DataFrame, spark: SparkSession) = {
    val format = parseFormat(output)
    format match {
      case Formats.parquet => df.write.parquet(output)
      case Formats.csv => df.write.csv(output)
      case Formats.console => df.show()
      case _ => throw new Exception(s"Unknown save format: $format")
    }
  }

  private def parseFormat(output: String): String = {
    output.split('.').last
  }
}

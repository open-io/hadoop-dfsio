package io.openio.spark

import java.io.File

import com.typesafe.config.ConfigFactory
import io.openio.spark.utils.Utils
import io.openio.spark.utils.Utils.getOrThrow
import io.openio.spark.workload.{KMeansGenWorkload, SQLWorkload}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object Run extends App {
  override def main(args: Array[String]): Unit = {
    if (args.isEmpty) throw new IllegalArgumentException("No arguments")
    val configPath = args.head
    val file = new File(configPath)
    if (!file.exists()) throw new IllegalArgumentException(s"No such file: $configPath")

    val workloadConfig = parseConfig(file)
    runWorkload(workloadConfig)
  }

  private def runWorkload(opts: Map[String, Any]) = {
    val workloads = Set(
      SQLWorkload,
      KMeansGenWorkload
    ).map(w => w.name -> w).toMap

    val workloadName = getOrThrow[String](opts, "name")
    val workload = workloads.getOrElse(workloadName,
      throw new IllegalArgumentException(s"No such workload: $workloadName"))
    val spark = createSparkSession()
    val result = workload(opts).run(spark)
    result.show()
  }

  private def parseConfig(file: File) : Map[String, Any] = {
    val config = ConfigFactory.parseFile(file)
    val unwrapped = config.root().unwrapped().asScala.toMap
    unwrapped
  }

  private def createSparkSession(): SparkSession = {
    SparkSession.builder().getOrCreate()
  }
}
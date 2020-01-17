package io.openio.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

trait WorkloadDefaults {
  val name: String

  def apply(m: Map[String, Any]): Workload
}

trait Workload {
  val input: Option[String]
  val output: Option[String]

  def doWork(df: Option[DataFrame], spark: SparkSession): DataFrame
}

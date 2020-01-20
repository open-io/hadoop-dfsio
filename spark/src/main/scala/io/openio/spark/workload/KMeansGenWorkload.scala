package io.openio.spark.workload

import io.openio.spark.utils.SparkUtils.writeOutput
import io.openio.spark.utils.Utils.{getOrDefault, getOrThrow, measureTime}
import io.openio.spark.{Workload, WorkloadDefaults}
import org.apache.spark.mllib.util.KMeansDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


case class KMeansGenWorkloadResult(
                                    name: String,
                                    timestamp: Long,
                                    genTime: Long,
                                    convertTime: Long,
                                    writeTime: Long,
                                    totalTime: Long
                                  )

object KMeansGenWorkload extends WorkloadDefaults {
  val name = "kmeans-gen"
  val numClusters: Int = 2
  val scaling: Double = 0.6
  val numPartitions: Int = 2

  def apply(m: Map[String, Any]): KMeansGenWorkload = new KMeansGenWorkload(
    numRows = getOrThrow[Int](m, "rows"),
    numCols = getOrThrow[Int](m, "cols"),
    output = Some(getOrThrow[String](m, "output")),
    k = getOrDefault[Int](m, "k", numClusters),
    scaling = getOrDefault[Double](m, "scaling", scaling),
    numPartitions = getOrDefault[Int](m, "partitions", numPartitions)
  )
}

case class KMeansGenWorkload(
                              input: Option[String] = None,
                              output: Option[String],
                              numRows: Int,
                              numCols: Int,
                              k: Int,
                              scaling: Double,
                              numPartitions: Int
                            ) extends Workload {

  override def doWork(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    val timestamp = System.nanoTime()

    val (genTime, data): (Long, RDD[Array[Double]]) = measureTime {
      KMeansDataGenerator.generateKMeansRDD(
        spark.sparkContext,
        numRows,
        k,
        numCols,
        scaling,
        numPartitions
      )
    }

    val (convertTime, dataDF) = measureTime {
      // Generate schema from data
      val schemaString = data.first().indices.map(i => "col_" + i.toString).mkString(" ")
      val fields = schemaString.split(" ")
        .map(fieldName => StructField(fieldName, DoubleType, nullable = false))
      val schema = StructType(fields)
      // Convert records to Rows
      val rowRDD = data.map(arr => Row(arr: _*))
      // Apply the schema to the RDD
      spark.createDataFrame(rowRDD, schema)
    }

    val (writeTime, _) = measureTime {
      writeOutput(output.get, dataDF, spark)
    }

    val totalTime = genTime + convertTime + writeTime

    val workloadResult = KMeansGenWorkloadResult(
      "kmeans-gen",
      timestamp,
      genTime,
      convertTime,
      writeTime,
      totalTime
    )
    spark.createDataFrame(Seq(workloadResult))
  }
}
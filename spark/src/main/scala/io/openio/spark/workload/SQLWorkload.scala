package io.openio.spark.workload

import io.openio.spark.Workload
import io.openio.spark.utils.SparkUtils.{loadInput, writeOutput}
import io.openio.spark.utils.Utils.measureTime
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SQLWorkloadResult(
                              name: String,
                              timestamp: Long,
                              loadTime: Long,
                              queryTime: Long,
                              writeTime: Long = 0L,
                              totalTime: Long
                            )

case class SQLWorkload(input: Option[String],
                       output: Option[String] = None,
                       queryStr: String
                      ) extends Workload {

  override def doWork(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    val timestamp = System.nanoTime()

    val (loadTime, df) = measureTime {
      loadInput(input.get, spark)
    }

    val (queryTime, result) = measureTime {
      df.createOrReplaceTempView("input")
      spark.sqlContext.sql(queryStr)
    }

    val (writeTime, _) = output match {
      case Some(path) => measureTime {
        writeOutput(path, result, spark)
      }
      case _ => (0L, Unit)
    }

    val totalTime = loadTime + queryTime + writeTime

    val workloadResult = SQLWorkloadResult(
      "sql",
      timestamp,
      loadTime,
      queryTime,
      writeTime,
      totalTime
    )
    spark.createDataFrame(Seq(workloadResult))
  }
}

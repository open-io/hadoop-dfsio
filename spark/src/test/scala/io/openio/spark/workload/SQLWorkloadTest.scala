package io.openio.spark.workload

import io.openio.spark.test.TestFixtures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SQLWorkloadTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val testFixtures = new TestFixtures("sql")
  val spark = testFixtures.spark
  val dataPath = testFixtures.testFolderPath + "/data.parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    testFixtures.createWorkspace()
    testFixtures.generateData(1000, 10, dataPath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    testFixtures.deleteWorkspace()
  }

  "SQLWorkload" should "execute query on input" in {
    val output = "console"
    val workload = SQLWorkload(
      input = Some(dataPath),
      output = Some(output),
      queryStr = "select col_0 from input where col_0 < -0.9"
    )

    val workloadResult = workload.doWork(None, spark)
    workloadResult.show()
  }
}

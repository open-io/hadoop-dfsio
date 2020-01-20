package io.openio.spark.workload

import java.io.File

import io.openio.spark.test.TestFixtures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class KMeansGenWorkloadTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  val testFixtures = new TestFixtures("kmeans-gen")
  val spark = testFixtures.spark

  override def beforeAll(): Unit = {
    super.beforeAll()
    testFixtures.createWorkspace()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    testFixtures.deleteWorkspace()
  }

  "KMeansGenWorkload" should "generate a csv output" in {
    val output = testFixtures.testFolderPath + "/output.csv"
    val m = Map(
      "rows" -> 10,
      "cols" -> 10,
      "output" -> output
    )
    val workload = KMeansGenWorkload(m)

    val workloadResult = workload.doWork(spark = spark)
    workloadResult.show()
    val file = new File(output)
    val fileList = file.listFiles().toList
    val partList = fileList.filter(_.getName.startsWith("part"))

    partList.length should be > 0
    val count = spark.read.csv(output).count()
    count shouldBe 10
  }
}
package io.openio.spark.utils

object Utils {

  def measureTime[R](block: => R): (Long, R) = {
    val start = System.nanoTime();
    val result = block
    (System.nanoTime() - start, result)
  }

  def getOrDefault[A](m: Map[String, Any], key: String, default: A): A = {
    m.get(key) match {
      case Some(x) => x.asInstanceOf[A]
      case _ => default
    }
  }

  def getOrThrow(m: Map[String, Any], key: String): Any = getOrThrow(m.get(key))

  def getOrThrow[A](opt: Option[A]): A = opt match {
    case Some(x) => x
    case _ => throw new Exception("Empty Option")
  }

}

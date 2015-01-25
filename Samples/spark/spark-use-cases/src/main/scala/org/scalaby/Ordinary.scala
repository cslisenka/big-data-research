
package org.scalaby

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.scalaby.Helpers._

object Ordinary {
  def main(args: Array[String]) {    
    val conf = new SparkConf().setAppName("Ordinary")
    val sc = new SparkContext(conf)
    val path = sys.env("SPARK_HOME") + "/README.md" // SPARK_HOME is set by Spark itself
    // val path = "hdfs://..."
    val rdd = sc.textFile(path)
    val cnt = rdd.map(_.toLowerCase).filter(_.contains("spark")).count()
    println("Lines with 'spark': %s".format(cnt))
    sc.stop() // otherwise executor state will be considered killed
  }
}

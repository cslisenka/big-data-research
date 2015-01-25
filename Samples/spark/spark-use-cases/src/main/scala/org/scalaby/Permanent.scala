
package org.scalaby

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.scalaby.Helpers._

object Permanent {

  val NWorkers = 4

  def permanentCount(mapperId: Int): Unit = {
    var n = 0
    while (true) {
      n += 1
      println("Mapper %s: %s".format(mapperId, n))
      Thread.sleep(1000)
    }
  }


  def main(args: Array[String]) {    
    val conf = new SparkConf().setAppName("Permanent")
    val sc = new SparkContext(conf)
  
    val rdd = sc.parallelize(1 to 10, NWorkers).map(permanentCount(_))
    rdd.collect() // force computations

    sc.stop()
  }
}

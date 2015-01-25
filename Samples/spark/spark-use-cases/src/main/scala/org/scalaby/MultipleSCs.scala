
package org.scalaby

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.scalaby.Helpers._

object MultipleSCs {


  def main(args: Array[String]) {    

    val t1 = new Thread(
      new Runnable() {
        def run() {
          val conf = new SparkConf().setAppName("MultipleSCs_1")
          val sc = new SparkContext(conf)
          sc.parallelize(1 to 8, 4).map(n => println("thread 1, value: " + n)).collect()
          sc.stop()
        }
      }
    )

    val t2 = new Thread(
      new Runnable() {
        def run() {
          val conf = new SparkConf().setAppName("MultipleSCs_2")
          val sc = new SparkContext(conf)
          sc.parallelize(1 to 8, 4).map(n => println("thread 2, value: " + n)).collect()
          sc.stop()
        }
      }
    )

    t1.start()
    t2.start()

    t1.join()
    t2.join()
  }
}


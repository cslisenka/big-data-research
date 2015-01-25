
package org.scalaby

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.scalaby.Helpers._

object MultipleRDDs {


  // def mapper(word: String, mapperId: Int): Int = {
  //   for (i <- List(1, 2, 3, 4)) {
  //     println("Word: %s; mapper %s: ; i: %s".format(word, mapperId, i))
  //     Thread.sleep(1000)
  //   }
  //   return 100
  // }


  // def computeRDD(sc: SparkContext, word: String): Int = {
  //   new Thread(
  //     new Runnable() {
  //       def run() {
  //         sc.parallelize(1 to 8, 4).map(idx => mapper(word, idx))
  //       }
  //     }
  //   ).start()
  //   return 0
  // }


  def main(args: Array[String]) {    
    val conf = new SparkConf().setAppName("MultipleRDDs")
    conf.set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(conf)

    val sparkHome = sys.env("SPARK_HOME")
    val sourceRdd = sc.textFile(s"$sparkHome/README.md")

    val rdd1 = sourceRdd.filter(line => line.contains("a")).map(line => "line with A:" + line)
    val rdd2 = sourceRdd.filter(line => !line.contains("a")).map(line => "line without A:" + line)

    List(rdd1, rdd2).par.foreach { rdd =>
      rdd.collect().foreach(line => println(line))
    }

    // computeRDD(sc, "hello")
    // computeRDD(sc, "world")

    // val t1 = new Thread(
    //   new Runnable() {
    //     def run() {
    //       sc.parallelize(1 to 8, 4).map(n => for (i <- 1 to 10) { println("thread 1, value: " + n); Thread.sleep(1000) }).collect()
    //     }
    //   }
    // )

    // val t2 = new Thread(
    //   new Runnable() {
    //     def run() {
    //       sc.parallelize(1 to 8, 4).map(n => for (i <- 1 to 10) { println("thread 2, value: " + n); Thread.sleep(1000) } ).collect()
    //     }
    //   }
    // )

    // t1.start()
    // t2.start()

    // t1.join()
    // t2.join()

    // sc.stop()
  }
}

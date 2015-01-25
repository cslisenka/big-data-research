
package org.scalaby

object Helpers {

  def sparkHome(): String = {
    try {
      sys.env("SPARK_HOME")
    } catch {
      case e: Exception => {
        println("ERROR: SPARK_HOME environmet variable is empty. Please specify it and run again")
        e.printStackTrace
        exit(-1)
      }
      case e: Throwable => throw e
    }
  }

}

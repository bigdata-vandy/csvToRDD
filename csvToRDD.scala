import breeze.numerics.pow
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
/**
  * Created by Frederick Weitendorf on 8/11/16.
  */
class csvToRDD {
  def loadCSV(file: String, sc: SparkContext): RDD[Array[String]] = {
    val csv = sc.textFile(file): RDD[String] //each element is a single row
    //maps the row string into an array of strings previously separated by ","
    csv.map(line => line.split(","))
  }

  def removeHeader(wholeFile: RDD[Array[String]]): RDD[Array[String]] = {
    val header = wholeFile.first
    wholeFile.filter(_ (0) != header(0))
  }

  def removeString(input: RDD[Array[String]], removed: String): RDD[Array[String]] = {
    input.map(arr => removeStringPrivate(arr, removed))
  }

  private def removeStringPrivate(input: Array[String], removed: String): Array[String] = {
    for (i <- input.indices) {
      input(i) = input(i).replaceAll(removed, "")
    }
    input
  }

  def removeColumn(input: RDD[Array[String]], index: Int): RDD[Array[String]] = {
    input.map(arr => arr.drop(index))
  }

  def sciToNum(input: String): Double = {
    for (x <- 0 until input.length) {
      if (input(x) == 'E' || input(x) == 'e') {
        val significand = input.substring(0, x).toDouble
        val exponent = input.substring(x + 1).toDouble
        return significand * pow(10, exponent)
      }
    }
    input.toDouble
  }
}
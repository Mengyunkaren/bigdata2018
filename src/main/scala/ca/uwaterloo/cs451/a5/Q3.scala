package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "query date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q3 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val date = args.date()

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val partRDD = sc.textFile(args.input() + "/part.tbl")
      val supplierRDD = sc.textFile(args.input() + "/supplier.tbl")

      val buildPartRDDHashmap = partRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
      val partRDDHashmap = sc.broadcast(buildPartRDDHashmap)

      val buildSupplierRDDHashmap = supplierRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
      val supplierRDDHashmap = sc.broadcast(buildSupplierRDDHashmap)

      lineitemRDD
        .filter(line => {
          val tokens = line.split("\\|")
          tokens(10).contains(date)
        })
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0).toLong, (tokens(1), tokens(2)))
        })
        .sortByKey()
        .take(20)
        .map(pair => (pair._1, partRDDHashmap.value(pair._2._1), supplierRDDHashmap.value(pair._2._2)))
        .foreach(println)
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd

      val buildPartRDDHashmap = partRDD
        .map(line => (line(0), line(1)))
        .collectAsMap()
      val partRDDHashmap = sc.broadcast(buildPartRDDHashmap)

      val buildSupplierRDDHashmap = supplierRDD
        .map(line => (line(0), line(1)))
        .collectAsMap()
      val supplierRDDHashmap = sc.broadcast(buildSupplierRDDHashmap)

      lineitemRDD
        .filter(line => line(10).toString.contains(date))
        .map(line => (line(0).toString.toLong, (line(1), line(2))))
        .sortByKey()
        .take(20)
        .map(pair => (pair._1, partRDDHashmap.value(pair._2._1), supplierRDDHashmap.value(pair._2._2)))
        .foreach(println)
    }
  }
}

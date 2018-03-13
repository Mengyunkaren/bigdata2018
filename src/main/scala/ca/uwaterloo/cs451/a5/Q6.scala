package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "query date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q6 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val date = args.date()

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      lineitemRDD
        .filter(line => {
          val tokens = line.split("\\|")
          tokens(10).contains(date)
        })
        .map(line => {
          val tokens = line.split("\\|")
          val returnFlag = tokens(8)
          val lineStatus = tokens(9)
          val quantity = tokens(4).toLong
          val extendedPrice = tokens(5).toDouble
          val discount = tokens(6).toDouble
          val tax = tokens(7).toDouble

          val discPrice = extendedPrice * (1 - discount)
          val charge = discPrice * (1 + tax)
          val count = 1
          ((returnFlag, lineStatus), (quantity, extendedPrice, discPrice, charge, discount, count))
        })
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
        .sortByKey()
        .map(pair => (pair._1._1, pair._1._2, pair._2._1, pair._2._2, pair._2._3, pair._2._4,
            pair._2._1 / pair._2._6, pair._2._2 / pair._2._6, pair._2._5 / pair._2._6, pair._2._6))
        .collect()
        .foreach(println)
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      lineitemRDD
        .filter(line => line(10).toString.contains(date))
        .map(line => {
          val returnFlag = line(8).toString
          val lineStatus = line(9).toString
          val quantity = line(4).toString.toFloat.toLong
          val extendedPrice = line(5).toString.toDouble
          val discount = line(6).toString.toDouble
          val tax = line(7).toString.toDouble
          val discPrice = extendedPrice * (1 - discount)
          val charge = extendedPrice * (1 - discount) * (1 + tax)
          val count = 1
          ((returnFlag, lineStatus), (quantity, extendedPrice, discPrice, charge, discount, count))
        })
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
        .sortByKey()
        .map(pair => (pair._1._1, pair._1._2, pair._2._1, pair._2._2, pair._2._3, pair._2._4,
            pair._2._1 / pair._2._6, pair._2._2 / pair._2._6, pair._2._5 / pair._2._6, pair._2._6))
        .collect()
        .foreach(println)
    }
  }
}
package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "query date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q7 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf7(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val date = args.date()

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")

      val buildCustomerRDDHashmap = customerRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        }).collectAsMap()
      val customerRDDHashmap = sc.broadcast(buildCustomerRDDHashmap)

      val filteredLineitem = lineitemRDD
        .filter(line => {
          val tokens = line.split("\\|")
          tokens(10) > date
        })
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(5).toDouble * (1 - tokens(6).toDouble))
        })
        .reduceByKey(_+_)

      val filteredOrders = ordersRDD
        .filter(line => {
          val tokens = line.split("\\|")
          tokens(4) < date
        })
        .map(line => {
          val tokens = line.split("\\|")
          val name = customerRDDHashmap.value(tokens(1))
          val orderDate = tokens(4)
          val shipPriority = tokens(7)
          (tokens(0), (name, orderDate, shipPriority))
        })

      filteredLineitem.cogroup(filteredOrders)
        .filter(pair => pair._2._1.iterator.hasNext && pair._2._2.iterator.hasNext)
        .map(pair => {
          val orderKey = pair._1
          val revenue = pair._2._1.iterator.next()

          val ordersIterator = pair._2._2.iterator.next()
          val name = ordersIterator._1
          val orderDate = ordersIterator._2
          val shipPriority = ordersIterator._3
          (revenue, (orderKey, name, orderDate, shipPriority))
        })
        .sortByKey(false)
        .take(10)
        .map(pair => (pair._2._2, pair._2._1, pair._1, pair._2._3, pair._2._4))
        .foreach(println)
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd

      val buildCustomerRDDHashmap = customerRDD
        .map(line => (line(0), line(1))).collectAsMap()
      val customerRDDHashmap = sc.broadcast(buildCustomerRDDHashmap)

      val filteredLineitem = lineitemRDD
        .filter(line => line(10).toString > date)
        .map(line => (line(0), line(5).toString.toDouble * (1 - line(6).toString.toDouble)))
        .reduceByKey(_+_)

      val filteredOrders = ordersRDD
        .filter(line => line(4).toString < date)
        .map(line => {
          val name = customerRDDHashmap.value(line(1))
          val orderDate = line(4)
          val shipPriority = line(7)
          (line(0), (name, orderDate, shipPriority))
        })

      filteredLineitem.cogroup(filteredOrders)
        .filter(pair => pair._2._1.iterator.hasNext && pair._2._2.iterator.hasNext)
        .map(pair => {
          val orderKey = pair._1
          val revenue = pair._2._1.iterator.next()

          val ordersIterator = pair._2._2.iterator.next()
          val name = ordersIterator._1
          val orderDate = ordersIterator._2
          val shipPriority = ordersIterator._3
          (revenue, (orderKey, name, orderDate, shipPriority))
        })
        .sortByKey(false)
        .take(10)
        .map(pair => (pair._2._2, pair._2._1, pair._1, pair._2._3, pair._2._4))
        .foreach(println)
    }
  }
}
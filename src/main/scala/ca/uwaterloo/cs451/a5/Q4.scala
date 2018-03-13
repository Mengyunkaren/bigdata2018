package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "query date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q4 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val date = args.date()

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")
      val nationRDD = sc.textFile(args.input() + "/nation.tbl")

      val buildCustomerRDDHashmap = customerRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(3))
        })
        .collectAsMap()
      val customerRDDHashmap = sc.broadcast(buildCustomerRDDHashmap)

      val buildNationRDDHashmap = nationRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
      val nationRDDHashmap = sc.broadcast(buildNationRDDHashmap)

      val filteredLineitem = lineitemRDD
        .filter(line => {
          val tokens = line.split("\\|")
          tokens(10).contains(date)
        })
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), "a")
        })

      val processedOrders = ordersRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })

      processedOrders.cogroup(filteredLineitem)
        .filter(pair => pair._2._2.iterator.hasNext)
        .map(pair => {
          var i = 0
          var pairIterator = pair._2._2.iterator
          while (pairIterator.hasNext) {
            var any = pairIterator.next()
            i += 1
          }
          (pair._1, pair._2._1.iterator.next(), i)
        })
        .map(pair => (customerRDDHashmap.value(pair._2), pair._3))
        .reduceByKey(_+_)
        .map(pair => (pair._1.toInt, (nationRDDHashmap.value(pair._1), pair._2)))
        .sortByKey()
        .collect()
        .foreach(pair => println("(" + pair._1+ "," + pair._2._1 + "," + pair._2._2 + ")"))
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd

      val buildCustomerRDDHashmap = customerRDD
        .map(line => (line(0), line(3)))
        .collectAsMap()
      val customerRDDHashmap = sc.broadcast(buildCustomerRDDHashmap)

      val buildNationRDDHashmap = nationRDD
        .map(line => (line(0), line(1)))
        .collectAsMap()
      val nationRDDHashmap = sc.broadcast(buildNationRDDHashmap)

      val filteredLineitem = lineitemRDD
        .filter(line => line(10).toString.contains(date))
        .map(line => (line(0), "a"))

      val processedOrders = ordersRDD
        .map(line => (line(0), line(1)))

      processedOrders.cogroup(filteredLineitem)
        .filter(pair => pair._2._2.iterator.hasNext)
        .map(pair => {
          var i = 0
          var pairIterator = pair._2._2.iterator
          while (pairIterator.hasNext) {
            var any = pairIterator.next()
            i += 1
          }
          (pair._1, pair._2._1.iterator.next(), i)
        })
        .map(pair => (customerRDDHashmap.value(pair._2), pair._3))
        .reduceByKey(_+_)
        .map(pair => (pair._1.toString.toInt, (nationRDDHashmap.value(pair._1).toString, pair._2)))
        .sortByKey()
        .collect()
        .foreach(pair => println("(" + pair._1+ "," + pair._2._1 + "," + pair._2._2 + ")"))
    }
  }
}

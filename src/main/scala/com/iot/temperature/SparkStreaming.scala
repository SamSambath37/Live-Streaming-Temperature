package com.iot.temperature

import org.apache.spark.rdd.RDD

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Seconds, Minutes, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.phoenix.spark._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector.streaming._
import org.apache.phoenix.spark._


object SparkStreaming extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val kafkaZk = "localhost:2181"
  val phoenixZk = "localhost:2181"

  val conf = new SparkConf(true)
    .setAppName("SparkStreaming")
    .setMaster("local[4]")

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(3))

  val topics = Map("arduino" -> 1)

  val kafkaStream = KafkaUtils.createStream(ssc, kafkaZk, "SparkStreaming", topics)

  println("started")
  kafkaStream.foreachRDD { rdd =>
    rdd.foreach(f => println("key-Id = " + f._1 + "\n value = " + f._2))
    sc
      .parallelize(
        Seq(
          (rdd, rdd)))
      .saveToPhoenix("TEST", Seq("name", "city"), zkUrl = Some(phoenixZk))

    println("done")
  }
  ssc.start()
  ssc.awaitTermination()
  println("finished")
}
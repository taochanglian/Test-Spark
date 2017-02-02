package com.test.spark.kafka

import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by tao on 17/1/12.
  */
object KafkaDirectorWordCount {

  def main(args: Array[String]) {
    if(args.length < 3) {
      System.err.println(
        s"""
          | Usage:DirectKafkaWordCount <borkers> <topics> <groupId>
          | <borkers> is a list of one or more kafka brokers
          | <topics> is a list of one or more kafka topics to consumer from
          | <groupId> is a comsumer group
          """.stripMargin)
      System.exit(1)
    }
    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(brokers,topics,groupId) = args

    val conf = new SparkConf().setAppName("")
    conf.set("spark.streaming.kafka.maxRatePerPartition","5")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(2))

    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )







  }
}




































































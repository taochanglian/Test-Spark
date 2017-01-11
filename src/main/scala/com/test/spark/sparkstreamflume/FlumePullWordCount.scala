package com.test.spark.sparkstreamflume

import java.net.{InetSocketAddress}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tao on 17/1/11.
  */
//一般使用这种方式
object FlumePullWordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlumePullWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    //flume的地址
    val address = Seq(new InetSocketAddress("ip",8888))
    val ds = FlumeUtils.createPollingStream(ssc,address,StorageLevel.MEMORY_AND_DISK)

    val words = ds.flatMap(x=>new String(x.event.getBody.array()).split(" ").map((_,1)))
    val results = words.reduceByKey(_+_)

    results.print()
    ssc.start()
    ssc.awaitTermination()

  }
}

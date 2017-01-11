package com.test.spark.sparkstreamflume

import com.test.spark.streamwordcount.LogLevels
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tao on 17/1/11.
  */
//这种方式用的不多,因为createStream方法只能传入一个sparkstreaming的ip地址,效率很低
object FlumePushWordCount {
  def main(args: Array[String]) {
    LogLevels.setStreamLogLevels()

    val conf = new SparkConf().setAppName("FlumePushWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    //推送方式,flume向sparkstreaming发送数据
    val flumeStream = FlumeUtils.createStream(ssc,"ip",8888)
    //flume中的数据必需通过event.getBody方法取得内容
    val words = flumeStream.flatMap(x=>new String(x.event.getBody.array()).split(" ").map((_,1)))
    val result = words.reduceByKey(_+_)

    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

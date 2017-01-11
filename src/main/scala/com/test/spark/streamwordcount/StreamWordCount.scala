package com.test.spark.streamwordcount

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tao on 17/1/10.
  */
object StreamWordCount {
  def main(args: Array[String]) {
    //设置logLevel
    LogLevels.setStreamLogLevels()

    val conf = new SparkConf().setAppName("StreamWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    //接受数据
    val ds = ssc.socketTextStream("127.0.0.1",8888)
    //DStream是一个特殊的RDD
    val result = ds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //数据存起来活着打印
//    result.mapPartitions(it=> {
//      val connection = null
//      it.map(_)
//    })
      result.print()

    //启动ds,等待结束
    ssc.start()
    ssc.awaitTermination()

//接下来通过nc 命令开始写数据,就行了  nc -lk 8888
  }
}

























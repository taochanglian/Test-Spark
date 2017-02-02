package com.test.spark.kafka

import com.test.spark.streamwordcount.LogLevels
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
  * Created by tao on 17/1/12.
  */
object KafkaWordCount {
  def main(args: Array[String]) {
    LogLevels.setStreamLogLevels()
    val Array(zkQuorum,group,topics,numThreads) = args

    val conf = new SparkConf().setAppName("KafkaWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("path")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap);

    data.print()

    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    wordCounts.print()
    //如果不是打印,是写到redis中,那么
    //wordCounts.mapPartitions(it=>{
      //创建jedis链接


//    })

    ssc.start()
    ssc.awaitTermination()

  }

  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])])=> {
    iter.flatMap{case (x,y,z)=>Some(y.sum + z.getOrElse(0)).map(i=>(x,i))}
  }
}

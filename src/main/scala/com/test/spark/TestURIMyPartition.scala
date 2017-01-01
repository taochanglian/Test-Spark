package com.test.spark

import java.net.URL

import org.apache.spark.{Partitioner, SparkContext, SparkConf}

/**
  * Created by tao on 16/12/31.
  */

object TestURIMyPartition {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestURLNew").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile(args(0)).map(line=> {
      val content = line.split("\t")
      (content(1),1)
    })

    val rdd2 = rdd1.reduceByKey(_+_)

    val rdd3 = rdd2.map(t=> {
      val url = t._1
      val host = new URL(url).getHost
      (host,(url,t._2))//如果要自定义Partitioner,要保证是一个k-v结构
    })

    val rdd4 = rdd3.map(_._1).distinct().collect()//获取所有的学院
  }
}

class HostPartition extends Partitioner {
  //从规则库中获取分区数量
  override def numPartitions: Int = ???

  override def getPartition(key: Any): Int = ???
}

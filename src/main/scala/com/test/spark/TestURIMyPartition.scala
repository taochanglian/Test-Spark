package com.test.spark

import java.net.URL

import org.apache.spark.{Partitioner, SparkContext, SparkConf}

import scala.collection.immutable.HashMap

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

    val rdd4 = rdd3.map(_._1).distinct().collect()//获取所有的学院,然后从这里开始,设置分区

    val hostPartition = new HostPartition(rdd4)

    //rdd3.partitionBy(hostPartition).saveAsTextFile("") 这个方法已经可以分区了,但是没有排序

    //分区+排序,注意mapPartitions方法,输入一个It,返回一个It
    val rddIt = rdd3.partitionBy(hostPartition).mapPartitions(it=>{
      it.toList.sortBy(_._2._2).reverse.take(3).iterator
    })
    rddIt.saveAsTextFile("")

    sc.stop()
  }


}

class HostPartition(hostPartition:Array[String]) extends Partitioner {

  val parMap = new scala.collection.mutable.HashMap[String,Int]()
  var count = 0
  for(i<-hostPartition) {
    parMap += (i->count)
    count+=1
  }

  override def numPartitions: Int = hostPartition.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString,0)
  }
}

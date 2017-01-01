package com.test.spark

import java.net.URL

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tao on 16/12/30.
  */

object TestURINew {
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
      (host,url,t._2)
    })

    val javaRdd = rdd3.filter(_._1.equals("java.itcast.cn"))
    val javaSort = javaRdd.sortBy(_._3).take(3)



  }
}





























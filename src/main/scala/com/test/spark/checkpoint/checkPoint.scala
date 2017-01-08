package com.test.spark.checkpoint

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tao on 17/1/8.
  */
object checkPoint {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ck")
    val sc = new SparkContext(conf)
    //首先设置checkpoint的路径
    sc.setCheckpointDir("hdfs:path");

    val rdd = sc.textFile("xx").flatMap(_.split("\t")).map((_,1)).reduceByKey(_+_)
    rdd.checkpoint()

    //当执行action的时候,会启动两个计算任务.一个是count自身的计算任务,还有一个计算任务是将数据写入至checkpoint
    rdd.collect()

  }
}

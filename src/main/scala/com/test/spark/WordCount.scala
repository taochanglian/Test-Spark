package com.test.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tao on 16/12/28.
  */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,true).saveAsTextFile(args(1))
    sc.stop()
  }
}

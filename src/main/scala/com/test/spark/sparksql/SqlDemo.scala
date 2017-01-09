package com.test.spark.sparksql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
  * Created by tao on 17/1/9.
  */
object SqlDemo {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("sqlDemo")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    System.setProperty("user.name","devel")

    val personRDD = sc.textFile("hdfs:path").map(line => {
      val fields = line.split(",")
      Person(fields(0).toLong,fields(1),fields(2).toInt)
    })

    //必需导入这个隐式转换
    import sqlContext.implicits._
    val personDF = personRDD.toDF

    personDF.registerTempTable("t_person")

    sqlContext.sql("SELECT * FROM t_person").show()

    sc.stop()



  }


}
//通过反射机制,及通过case class来定义DataFrame的metadata
case class Person(id:Long,name:String,age:Int)























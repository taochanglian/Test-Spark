package com.test.spark.sparksql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tao on 17/1/9.
  */
object SqlDemo2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SqlDemo2")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val personRDD = sc.textFile("filePath").map(_.split(" "))
    //通过StructType直接制定每个字段类型
    val schema = StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      )
    )
    //将RDD印社到rowRDD
    val rowRDD = personRDD.map(p=>Row(p(0).toInt,p(1).trim,p(2).toInt))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD,schema)

    //注册表
    personDataFrame.registerTempTable("t_person")

    val df = sqlContext.sql("select ...")

    df.write.parquet("")

    //以后可以从外部数据源中读取对应的计算结果
    sqlContext.read.parquet()
  }
}
















































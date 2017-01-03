package com.test.spark.JdbcRdd

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tao on 17/1/3.
  */
object JdbcRddDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JdbcRddDemo")
    val sc = new SparkContext(conf)

    def getConnection()={
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/db","root","root")
    }
    val jdbcRdd = new JdbcRDD(
      sc,
      getConnection,
      "SELECT * FROM table WHERE id >= ? AND id <= ?",
      1,4,2,
      rs=> {
        val id = rs.getInt(1)
        val code = rs.getString(2)
        (id,code)
      }
    )
    val jrdd = jdbcRdd.collect()
    println(jrdd.toBuffer)
    sc.stop()
  }
}






















































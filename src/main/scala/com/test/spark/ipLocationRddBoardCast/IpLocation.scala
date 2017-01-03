package com.test.spark.ipLocationRddBoardCast

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tao on 17/1/3.
  */
object IpLocation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("IpLocation")
    val sc = new SparkContext(conf)

    //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    val ipRulesRdd = sc.textFile("/ip.txt").map(line=> {
      val fields = line.split("\\|")
      val start_ipnum = fields(2)
      val end_ipnum = fields(3)
      val province = fields(6)
      (start_ipnum,end_ipnum,province)
    })
    //收集到Driver中,保存了全部的ip规则库
    val ipRulesArr = ipRulesRdd.collect()
    //sc广播到各个execute
    val ipRulesBroadcast = sc.broadcast(ipRulesArr)

    //加载要处理的数据,获取要处理的ip
    val ipsRdd = sc.textFile("/x.http.format").map(line=> {
      val fields = line.split("\\|")
      fields(1)
    })

    val result = ipsRdd.map(ip=>{
      val ipNum = ip2Long(ip)
      //返回索引
      val index = binarySearch(ipRulesBroadcast.value,ipNum)
      val info = ipRulesBroadcast.value(index)
      info
    })

    println(result.collect.toBuffer)
    sc.stop()
  }





  //通过ip获取Long
  def ip2Long(ip:String):Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for(i<- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
  //二分查找
  def binarySearch(lines:Array[(String,String,String)],ip:Long):Int = {
    var low = 0
    var high = lines.length -1
    while(low <= high) {
      val middle = (low + high) / 2
      if((ip>=lines(middle)._1.toLong) && (ip<=lines(middle)._2.toLong))
        return middle
      if(ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

}

package com.test.spark.ipLocation

import java.io.{FileInputStream, InputStreamReader, BufferedReader}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tao on 17/1/3.
  */
//单机处理,获取ip,得到ip地址的省市
object IpLocationDemo {

  def ip2Long(ip:String):Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for(i<- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readData(path:String):ArrayBuffer[String] = {
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
    var s:String = null
    var flag = true
    val lines = new ArrayBuffer[String]()

    while(flag) {
      s = br.readLine()
      if(s!=null) {
        lines += s
      }else {
        flag = false
      }

    }
    lines
  }

  def binarySearch(lines:ArrayBuffer[String],ip:Long):Int = {
    var low = 0
    var high = lines.length -1
    while(low <= high) {
      val middle = (low + high) / 2
      if((ip>=lines(middle).split("\\|")(2).toLong) && (ip<=lines(middle).split("\\|")(3).toLong))
        return middle
      if(ip < lines(middle).split("\\|")(2).toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]) {
    val ip = "118.144.130.10"
    val ipNum = ip2Long(ip)
    println(ipNum)
    val lines = readData("/Users/tao/git/Test-Spark/src/main/resources/ip.txt")
    val index = binarySearch(lines,ipNum)
    println(lines(index))

  }
}
















































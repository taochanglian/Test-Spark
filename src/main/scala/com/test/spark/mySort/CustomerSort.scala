package com.test.spark.mySort

import org.apache.spark.{SparkContext, SparkConf}

//第二种自定义排序方式,采用隐式转换
//object OrderContext {
//  implicit object GirlOrdering extends Ordering[Girl] {
//    override def compare(x: Girl, y: Girl): Int = {
//      if(x.faceValue > y.faceValue) 1
//      else if (x.faceValue==y.faceValue) {
//        if(x.age > y.age) -1 else 1
//      } else -1
//    }
//  }
//}

/**
  * Created by tao on 17/1/2.
  */

//Person
//name,faceValue,age
//sort:首先按faceValue排序,如果相等,再比较年龄
object CustomerSort {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CustomerSort").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("zhangsan",90,28,1),("lisi",90,27,2),("wangwu",95,22,3)))

    //第一种自定义排序方式,建立一个Girl,然后继承Ordered,然后写排序内容
    val rdd2 = rdd1.sortBy(x=>Girl(x._2,x._3),false)

    //第二种方式
//  import OrderContext._
//  val rdd2 = rdd1.sortBy(x=>Girl(x._2,x._3),false)
    println(rdd2.collect().toBuffer)
    sc.stop

  }

}

//第一种自定义排序方式

case class Girl(val faceValue:Int,val age:Int) extends Ordered[Girl] with Serializable {
  override def compare(that: Girl): Int = {
    if(this.faceValue == that.faceValue) {
      this.age - this.age
    }else {
      this.faceValue - that.faceValue
    }
  }
}

//第二种
//case class Girl(faceValue:Int,age:Int) extends Serializable





































































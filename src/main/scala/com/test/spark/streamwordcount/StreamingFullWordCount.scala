package com.test.spark.streamwordcount

import org.apache.spark.streaming.dstream.{StateDStream, DStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Partitioner, HashPartitioner, SparkContext, SparkConf}

import scala.reflect.ClassTag

/**
  * Created by tao on 17/1/11.
  */
object StreamingFullWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StreamingFullWordCount")
    val sc = new SparkContext(conf)
    //如果使用updateStateBykey,必需设置checkpoint
    sc.setCheckpointDir("path")//一定要设置
    val ssc = new StreamingContext(sc,Seconds(5))

    //数据源
    val ds = ssc socketTextStream("ip", 8888)
    //计算.注意:以前是reducebykey,现在是updateStateByKey
    //如果使用updateStateBykey,必需设置checkpoint
    val result = ds.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),true)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

//---------------------------------------------------------------------------------------------------------------
  /**
    *    * Return a new "state" DStream where the state for each key is updated by applying
    * the given function on the previous state of the key and the new values of each key.
    * org.apache.spark.Partitioner is used to control the partitioning of each RDD.
    *
    * @param updateFunc State update function. Note, that this function may generate a different
    *                   tuple with a different key than the input key. Therefore keys may be removed
    *                   or added in this way. It is up to the developer to decide whether to
    *                   remember the partitioner despite the key being changed.
    * @param partitioner Partitioner for controlling the partitioning of each RDD in the new
    *                    DStream
    * @param rememberPartitioner Whether to remember the paritioner object in the generated RDDs.
    * @tparam S State type
    */
//  def updateStateByKey[S: ClassTag](
//                                     updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
//                                     partitioner: Partitioner,
//                                     rememberPartitioner: Boolean
//                                   ): DStream[(K, S)] = ssc.withScope {
//    new StateDStream(self, ssc.sc.clean(updateFunc), partitioner, rememberPartitioner, None)
//  }
//it._1 key it._2 本批次中改key的所有值 it._3 上一个批次的值(针对于第一个批次,根据业务逻辑,初始化it._3,wordcount业务中,it._3是0)
//-------------------------------------------------------------------------------------------------------------------------
  //分好组的数据
  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    //iter.flatMap{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x,m))}
    iter.map(t=>(t._1,t._2.sum + t._3.getOrElse(0)))

  }

}

























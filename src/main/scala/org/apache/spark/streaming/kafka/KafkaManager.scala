package org.apache.spark.streaming.kafka

/**
  * Created by tao on 17/1/13.
  */
class KafkaManager(val kafkaParams:Map[String,String]) extends Serializable {
  private val kc = new KafkaCluster(kafkaParams)


}

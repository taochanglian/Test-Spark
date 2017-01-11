package com.test.spark.streamwordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
  * Created by tao on 17/1/10.
  */
object LogLevels extends Logging{

  def setStreamLogLevels(): Unit ={
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if(!log4jInitialized) {
      logInfo("Setting log level to [WARn] from streaming example. To override add a customer log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}

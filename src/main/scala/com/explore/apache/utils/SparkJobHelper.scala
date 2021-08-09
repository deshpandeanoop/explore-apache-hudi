package com.explore.apache.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Component contains common/helper methods related to
 * spark framework.
 */
object SparkJobHelper {

  /**
   * Creates local <code>sparkConf</code> bean with
   * given application name
   *
   * @param appName application name
   * @return local <code>sparkConf</code> bean
   */
  def getLocalSparkConf(appName : String) : SparkConf = {
    val sparkConf = new SparkConf
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName(appName)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sparkConf
  }

  /**
   * Creates local <code>sparkContext</code> bean
   *
   * @param appName application name
   * @return local <code>sparkContext</code> bean
   */
  def getLocalSparkContext(appName : String) : SparkContext = {
    new SparkContext(getLocalSparkConf(appName))
  }

  /**
   * Creates local <code>sparkSession</code> bean
   *
   * @param appName application name
   * @return local <code>sparkSession</code> bean
   */
  def getLocalSparkSession(appName : String) : SparkSession = {
    SparkSession
      .builder()
      .config(getLocalSparkConf(appName))
      .getOrCreate()
  }
}
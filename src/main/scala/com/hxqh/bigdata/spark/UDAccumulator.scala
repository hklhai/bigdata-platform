package com.hxqh.bigdata.spark

import com.hxqh.bigdata.jdbc.Constants
import com.hxqh.bigdata.util.StringUtils
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/12/29.
  */
object UDAccumulator {

  def main(args: Array[String]): Unit = {
    object UDSessionAccumulator extends AccumulatorParam[String] {

      override def zero(initialValue: String): String = {
        Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0"
      }

      override def addInPlace(r1: String, r2: String): String = {
        if (r1 == "") r2
        else {
          val oldValue = StringUtils.getFieldFromConcatString(r1, "\\|", r2)
          val newValue = Integer.valueOf(oldValue) + 1
          StringUtils.setFieldInConcatString(r1, "\\|", r2, String.valueOf(newValue))
        }
      }
    }

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("UDAccumulator"))

    // 使用accumulator()()方法（curry），创建自定义的Accumulator
    val sessionAccumulator = sc.accumulator("")(UDSessionAccumulator)

    val arr = Array(Constants.TIME_PERIOD_1s_3s, Constants.TIME_PERIOD_4s_6s)
    val rdd = sc.parallelize(arr, 1)
    rdd.foreach(sessionAccumulator.add(_))
    println(sessionAccumulator.value)
  }
}

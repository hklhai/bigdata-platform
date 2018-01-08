package com.hxqh.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2018/1/8.
  *
  * @author Ocean lin
  */
object SortKeyTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("SortKeyTest"))

    val arr = Array(Tuple2(new SortKey(30, 35, 40), "1"),
      Tuple2(new SortKey(35, 30, 40), "2"),
      Tuple2(new SortKey(30, 38, 30), "3"))

    val rdd = sc.parallelize(arr, 1)

    val sortedRDD = rdd.sortByKey(false)

    for(t <- sortedRDD){
      println(t._2)
    }
  }
}

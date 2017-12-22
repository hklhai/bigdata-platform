package com.hxqh.bigdata.spark;

import com.hxqh.bigdata.conf.ConfigurationManager;
import com.hxqh.bigdata.jdbc.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Ocean lin on 2017/12/22.
 */
public class UserVisitSessionAnalysis {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(Constants.SPARK_APP_NAME_SESSION);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc.sc());
        // 生成模拟测试数据
        mockData(sc, sqlContext);



        sc.close();
    }


    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }
}

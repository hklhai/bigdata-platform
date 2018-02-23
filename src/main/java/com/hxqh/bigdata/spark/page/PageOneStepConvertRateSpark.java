package com.hxqh.bigdata.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.hxqh.bigdata.common.Constants;
import com.hxqh.bigdata.conf.ConfigurationManager;
import com.hxqh.bigdata.util.ParamUtils;
import com.hxqh.bigdata.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;
import java.util.Properties;

/**
 * Created by Ocean lin on 2018/2/23.
 *
 * @author Ocean lin
 */
public class PageOneStepConvertRateSpark {


    public static void main(String[] args) {
        // 1. 上下文
        SparkConf sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        // 2. 生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 3. 查询获取任务参数
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
        Properties prop = new Properties();
        prop.setProperty("user", ConfigurationManager.getProperty("jdbc.user"));
        prop.setProperty("password", ConfigurationManager.getProperty("jdbc.password"));
        DataFrame dataFrame = sqlContext.read().jdbc(ConfigurationManager.getProperty("jdbc.url"),
                "task", new String[]{"task_id=" + taskId}, prop).select("task_param");
        List<Row> rows = dataFrame.toJavaRDD().collect();
        String taskParameter = rows.get(0).getString(0);
        JSONObject taskParam = JSONObject.parseObject(taskParameter);
        // 4. 指定日期范围内的用户访问行为数据
        SparkUtils.getActionRDDByDateRange(sqlContext, taskParam)


        sc.close();
    }
}

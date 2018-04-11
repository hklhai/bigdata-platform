package com.hxqh.bigdata.sql;

import com.hxqh.bigdata.util.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 新闻网站关键指标离线统计Spark作业
 * <p>
 * Created by Ocean lin on 2018/4/3.
 *
 * @author Ocean lin
 */
public class NewsOfflineStatSpark {


    public static void main(String[] args) {
        // 创建SparkConf以及Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName("NewsOfflineStatSpark")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        // 页面pv统计以及排序
        // 开发第一个关键指标：页面pv统计以及排序
        calculateDailyPagePv(hiveContext, "2016-02-20");

        // 开发第二个关键指标：页面uv统计以及排序
        calculateDailyPageUv(hiveContext, "2016-02-20");

        // 开发第三个关键指标：新用户注册比率统计
        calculateDailyNewUserRegisterRate(hiveContext, "2016-02-20");

        // 开发第四个关键指标：用户跳出率统计
        calculateDailyUserJumpRate(hiveContext, "2016-02-20");
        // 开发第五个关键指标：版块热度排行榜
        calculateDailySectionPvSort(hiveContext, "2016-02-20");


        // 关闭Spark上下文
        sc.close();
    }

    /**
     * 计算每天每个页面的pv以及排序
     * 排序的好处：排序后，插入mysql，java web系统要查询每天pv top10的页面，直接查询mysql表limit 10即可
     * 如果这里不排序，那么java web系统就要做排序，反而会影响java web系统的性能以及用户响应用户时间
     *
     * @param hiveContext
     * @param date
     */
    private static void calculateDailyPagePv(HiveContext hiveContext, String date) {
        String sql =
                "SELECT "
                        + "date,"
                        + "pageid,"
                        + "pv "
                        + "FROM ( "
                        + "SELECT "
                        + "date,"
                        + "pageid,"
                        + "count(*) pv "
                        + "FROM news_access "
                        + "WHERE action='view' "
                        + "AND date='" + date + "' "
                        + "GROUP BY date,pageid "
                        + ") t "
                        + "ORDER BY pv DESC ";

        DataFrame df = hiveContext.sql(sql);

        // 在这里，可以转换成一个RDD，然后对RDD执行一个foreach算子
        // 在foreach算子中，将数据写入mysql中
        df.show();
    }

    /**
     * 计算每天每个页面的uv以及排序
     * Spark SQL的count(distinct)语句，有bug，默认会产生严重的数据倾斜
     * 只会用一个task，来做去重和汇总计数，性能很差
     * <p>
     * 先用最内层去重date,pageid,userid ，然后group by  date,pageid计算UV
     *
     * @param hiveContext
     * @param date
     */
    private static void calculateDailyPageUv(
            HiveContext hiveContext, String date) {
        String sql =
                "SELECT "
                        + "date,"
                        + "pageid,"
                        + "uv "
                        + "FROM ( "
                        + "SELECT "
                        + "date,"
                        + "pageid,"
                        + "count(*) uv "
                        + "FROM ( "
                        + "SELECT "
                        + "date,"
                        + "pageid,"
                        + "userid "
                        + "FROM news_access "
                        + "WHERE action='view' "
                        + "AND date='" + date + "' "
                        + "GROUP BY date,pageid,userid "
                        + ") t2 "
                        + "GROUP BY date,pageid "
                        + ") t "
                        + "ORDER BY uv DESC ";
        DataFrame df = hiveContext.sql(sql);

        df.show();
    }

    /**
     * 计算每天的新用户注册比例
     *
     * @param hiveContext
     * @param date
     */
    private static void calculateDailyNewUserRegisterRate(
            HiveContext hiveContext, String date) {
        // 昨天所有访问行为中，userid为null，新用户的访问总数
        String sql1 = "SELECT count(*) FROM news_access WHERE action='view' AND date='" + date + "' AND userid IS NULL";
        // 昨天的总注册用户数
        String sql2 = "SELECT count(*) FROM news_access WHERE action='register' AND date='" + date + "' ";

        // 执行两条SQL，获取结果
        Object result1 = hiveContext.sql(sql1).collect()[0].get(0);
        long number1 = 0L;
        if (result1 != null) {
            number1 = Long.valueOf(String.valueOf(result1));
        }

        Object result2 = hiveContext.sql(sql2).collect()[0].get(0);
        long number2 = 0L;
        if (result2 != null) {
            number2 = Long.valueOf(String.valueOf(result2));
        }

        double rate = (double) number2 / (double) number1;
        System.out.println("======================" + NumberUtils.formatDouble(rate, 2) + "======================");
    }


    /**
     * 计算每天的用户跳出率 = 已注册用户的昨天访问总数为1总数/已注册用户的昨天的总的访问pv
     *
     * @param hiveContext
     * @param date
     */
    private static void calculateDailyUserJumpRate(
            HiveContext hiveContext, String date) {
        // 计算已注册用户的昨天的总的访问pv
        String sql1 = "SELECT count(*) FROM news_access WHERE action='view' AND date='" + date + "' AND userid IS NOT NULL ";

        // 已注册用户的昨天跳出的总数 访问总数为1的注册用户数量
        String sql2 = "SELECT count(*) FROM ( SELECT count(*) cnt FROM news_access WHERE action='view' AND date='" + date + "' AND userid IS NOT NULL GROUP BY userid HAVING cnt=1 ) t ";

        // 执行两条SQL，获取结果
        Object result1 = hiveContext.sql(sql1).collect()[0].get(0);
        long number1 = 0L;
        if (result1 != null) {
            number1 = Long.valueOf(String.valueOf(result1));
        }

        Object result2 = hiveContext.sql(sql2).collect()[0].get(0);
        long number2 = 0L;
        if (result2 != null) {
            number2 = Long.valueOf(String.valueOf(result2));
        }

        double rate = (double) number2 / (double) number1;
        System.out.println("======================" + NumberUtils.formatDouble(rate, 2) + "======================");
    }

    /**
     * 计算每天的版块热度排行榜
     * 按照日期和板块分组
     *
     * @param hiveContext
     * @param date
     */
    private static void calculateDailySectionPvSort(
            HiveContext hiveContext, String date) {
        String sql =
                "SELECT "
                        + "date,"
                        + "section,"
                        + "pv "
                        + "FROM ( "
                        + "SELECT "
                        + "date,"
                        + "section,"
                        + "count(*) pv "
                        + "FROM news_access "
                        + "WHERE action='view' "
                        + "AND date='" + date + "' "
                        + "GROUP BY date,section "
                        + ") t "
                        + "ORDER BY pv DESC ";

        DataFrame df = hiveContext.sql(sql);

        df.show();
    }

}

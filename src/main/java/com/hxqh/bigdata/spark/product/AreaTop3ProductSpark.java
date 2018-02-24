package com.hxqh.bigdata.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.hxqh.bigdata.common.Constants;
import com.hxqh.bigdata.conf.ConfigurationManager;
import com.hxqh.bigdata.util.ParamUtils;
import com.hxqh.bigdata.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Ocean lin on 2018/2/23.
 * 各区域top3热门商品统计Spark作业
 *
 * @author Ocean lin
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args) {
        // 1. 上下文
        SparkConf sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PRODUCT);
        SparkUtils.setMaster(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());


        /**
         * 注册自定义函数
         */
        sqlContext.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());


        // 2. 生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 3. 查询获取任务参数
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        Properties prop = new Properties();
        prop.setProperty("user", ConfigurationManager.getProperty("jdbc.user"));
        prop.setProperty("password", ConfigurationManager.getProperty("jdbc.password"));
        DataFrame dataFrame = sqlContext.read().jdbc(ConfigurationManager.getProperty("jdbc.url"),
                "task", new String[]{"task_id=" + taskId}, prop).select("task_param");
        List<Row> rows = dataFrame.toJavaRDD().collect();
        String taskParameter = rows.get(0).getString(0);
        JSONObject taskParam = JSONObject.parseObject(taskParameter);

        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);


        // 查询用户指定日期范围内的点击行为数据（city_id，在哪个城市发生的点击行为）
        // 技术点1：Hive数据源的使用
        JavaPairRDD<Long, Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(sqlContext, startDate, endDate);
        System.out.println("cityid2clickActionRDD: " + cityid2clickActionRDD.count());

        // 从MySQL中查询城市信息
        // 技术点2：异构数据源中MySQL的使用
        JavaPairRDD<Long, Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);
        System.out.println("cityid2cityInfoRDD: " + cityid2cityInfoRDD.count());


        // 生成点击商品基础信息临时表
        // 技术点3：将RDD转换为DataFrame，并注册临时表
        generateTempClickProductBasicTable(sqlContext, cityid2clickActionRDD, cityid2cityInfoRDD);


        // 生成各区域各商品点击次数的临时表
        generateTempAreaPrdocutClickCountTable(sqlContext);

        sc.close();
    }


    /**
     * 查询指定日期范围内的点击行为数据
     *
     * @param sqlContext
     * @param startDate  起始日期
     * @param endDate    截止日期
     * @return 点击行为数据
     */
    private static JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(
            SQLContext sqlContext, String startDate, String endDate) {
        // 从user_visit_action中，查询用户访问行为数据
        // 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
        // 第二个限定：在用户指定的日期范围内的数据
        String sql =
                "SELECT "
                        + "city_id,"
                        + "click_product_id product_id "
                        + "FROM user_visit_action "
                        + "WHERE click_product_id IS NOT NULL "
                        + "AND date>='" + startDate + "' "
                        + "AND date<='" + endDate + "'";

        DataFrame clickActionDF = sqlContext.sql(sql);

        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(

                new PairFunction<Row, Long, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        Long cityid = row.getLong(0);
                        return new Tuple2<>(cityid, row);
                    }

                });

        return cityid2clickActionRDD;
    }


    /**
     * 使用Spark SQL从MySQL中查询城市信息
     *
     * @param sqlContext SQLContext
     * @return
     */
    private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
        // 构建MySQL连接配置信息（直接从配置文件中获取）
        String url = null;
        String user = null;
        String password = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String, String> options = new HashMap<>();
        options.put("url", url);
        options.put("dbtable", "city_info");
        options.put("user", user);
        options.put("password", password);

        // 通过SQLContext去从MySQL中查询数据
        DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();

        // 返回RDD
        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                long cityid = Long.valueOf(String.valueOf(row.get(0)));
                return new Tuple2<>(cityid, row);
            }
        });
        return cityid2cityInfoRDD;
    }


    /**
     * 生成点击商品基础信息临时表
     *
     * @param sqlContext
     * @param cityid2clickActionRDD
     * @param cityid2cityInfoRDD
     */
    private static void generateTempClickProductBasicTable(SQLContext sqlContext,
                                                           JavaPairRDD<Long, Row> cityid2clickActionRDD,
                                                           JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
        // 执行join操作，进行点击行为数据和城市数据的关联
        JavaPairRDD<Long, Tuple2<Row, Row>> joinRDD = cityid2clickActionRDD.join(cityid2cityInfoRDD);

        JavaRDD<Row> mappedRDD = joinRDD.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {

            @Override
            public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
                long cityId = tuple._1;
                Row clickAction = tuple._2._1;
                Row cityInfo = tuple._2._2;

                long productId = clickAction.getLong(1);
                String cityName = cityInfo.getString(1);
                String area = cityInfo.getString(2);

                return RowFactory.create(cityId, cityName, area, productId);
            }
        });

        // 基于JavaRDD<Row>的格式，才可以将其转换为DataFrame
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

        // 1 北京
        // 2 上海
        // 1 北京
        // group by area,product_id
        // 1:北京,2:上海

        // 两个函数
        // UDF：concat2()，将两个字段拼接起来，用指定的分隔符
        // UDAF：group_concat_distinct()，将一个分组中的多个字段值，用逗号拼接起来，同时进行去重
        StructType schema = DataTypes.createStructType(structFields);
        DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);
        System.out.println("tmp_click_product_basic: " + df.count());

        // 将DataFrame中的数据，注册成临时表（tmp_click_product_basic）
        df.registerTempTable("tmp_click_product_basic");
    }

    /**
     * 生成各区域各商品点击次数临时表
     *
     * @param sqlContext
     */
    private static void generateTempAreaPrdocutClickCountTable(
            SQLContext sqlContext) {
        // 按照area和product_id两个字段进行分组
        // 计算出各区域各商品的点击次数
        // 可以获取到每个area下的每个product_id的城市信息拼接起来的串
        String sql =
                "SELECT "
                        + "area,"
                        + "product_id,"
                        + "count(*) click_count, "
                        + "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos "
                        + "FROM tmp_click_product_basic "
                        + "GROUP BY area,product_id ";


        // 使用Spark SQL执行这条SQL语句
        DataFrame df = sqlContext.sql(sql);
        System.out.println("tmp_area_product_click_count: " + df.count());

        // 再次将查询出来的数据注册为一个临时表
        // 各区域各商品的点击次数（以及额外的城市列表）
        df.registerTempTable("tmp_area_product_click_count");
    }

}

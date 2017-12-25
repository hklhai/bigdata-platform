package com.hxqh.bigdata.spark;

import com.alibaba.fastjson.JSONObject;
import com.hxqh.bigdata.conf.ConfigurationManager;
import com.hxqh.bigdata.jdbc.Constants;
import com.hxqh.bigdata.util.ParamUtils;
import com.hxqh.bigdata.util.StringUtils;
import com.hxqh.bigdata.util.ValidUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created by Ocean lin on 2017/12/22.
 * <p>
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * <p>
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * <p>
 * <p>
 * Spark作业接受用户创建的任务
 * <p>
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 * <p>
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 */
public class UserVisitSessionAnalysis {

    public static void main(String[] args) {
        args = new String[]{"1"};

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(Constants.SPARK_APP_NAME_SESSION);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = getSQLContext(sc);
        // 生成模拟测试数据
        mockData(sc, sqlContext);


        /********************************获取查询参数**********************************/
        // 首先查询出来指定的任务，并获取任务的查询参数
        // todo 本地执行需要配置参数
        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Properties prop = new Properties();
        prop.setProperty("user", ConfigurationManager.getProperty("jdbc.user"));
        prop.setProperty("password", ConfigurationManager.getProperty("jdbc.password"));

        DataFrame dataFrame = sqlContext.read().jdbc(ConfigurationManager.getProperty("jdbc.url"),
                "task", new String[]{"task_id=" + taskid}, prop).select("task_param");
        List<Row> rows = dataFrame.toJavaRDD().collect();
        String task_param = rows.get(0).getString(0);
        JSONObject taskParam = JSONObject.parseObject(task_param);
        /********************************获取查询参数**********************************/
        JavaPairRDD<String, String> fullRDD = aggByUserId(sqlContext, taskParam);
        System.out.println("fullRDD:"+fullRDD.count());
        JavaPairRDD<String, String> filterRDDByParameter = filterRDDByParameter(taskParam, fullRDD);
        System.out.println("filterRDD:"+filterRDDByParameter.count());

        sc.close();
    }

    private static JavaPairRDD<String, String> filterRDDByParameter(JSONObject taskParam, JavaPairRDD<String, String> fullRDD) {
        // 使用ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // todo 可优化项
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
        if (_parameter.endsWith("\\|"))
            _parameter = _parameter.substring(0, _parameter.length() - 1);

        final String parameter = _parameter;


        // 获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> filter = fullRDD.filter(new Function<Tuple2<String, String>, Boolean>() {

            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String aggrInfo = v1._2;

                // 依次按照筛选条件进行过滤
                // 按照年龄范围进行过滤（startAge、endAge）
                if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    return false;
                }

                // 按照职业范围进行过滤（professionals）
                // 互联网,IT,软件
                // 互联网
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }

                // 按照城市范围进行过滤（cities）
                // 北京,上海,广州,深圳
                // 成都
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                    return false;
                }

                // 按照性别进行过滤
                // 男/女
                // 男，女
                if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                    return false;
                }

                // 按照搜索词进行过滤
                // 我们的session可能搜索了 火锅,蛋糕,烧烤
                // 我们的筛选条件可能是 火锅,串串香,iphone手机
                // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                // 任何一个搜索词相当，即通过
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }

                // 按照点击品类id进行过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }
                return true;
            }
        });
        return filter;
    }


    /**
     * 聚合为<SessionId,SessionId_words_catagorys>形式
     *
     * @param sqlContext
     * @param taskParamJson
     * @return
     */
    private static JavaPairRDD<String, String> aggByUserId(SQLContext sqlContext, JSONObject taskParamJson) {
        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParamJson);


        // 首先，以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后将session粒度的数据与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        JavaPairRDD<String, Row> pariRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
        JavaPairRDD<String, Iterable<Row>> sessionGroupRDD = pariRDD.groupByKey();

        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        // JavaPairRDD<Long, String> userId_Session_Word_CatagoryRDD =
        JavaPairRDD<Long, String> userId_Session_Word_CatagoryRDD = sessionGroupRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionId = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();

                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
                Long userid = null;
                String partAggrInfo = new String();

                // 遍历session所有的访问行为
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (userid == null)
                        userid = row.getLong(1);

                    String searchKeyword = row.getString(5);
                    Long clickCategoryId = row.getLong(6);

                    // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                    // 只有搜索行为，是有searchKeyword字段的;只有点击品类的行为，是有clickCategoryId字段的
                    // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                    // 是否将搜索词或点击品类id拼接到字符串中去
                    // 首先要满足2个条件：1.不能是null值；2.之前的字符串中还没有搜索词或者点击品类id
                    if (StringUtils.isNotEmpty(searchKeyword)) {
                        if (!searchKeywordsBuffer.toString().contains(searchKeyword))
                            searchKeywordsBuffer.append(searchKeyword + ",");
                    }
                    if (null != clickCategoryId) {
                        if (!(clickCategoryId.toString().contains(String.valueOf(clickCategoryId))))
                            clickCategoryIdsBuffer.append(clickCategoryId + ",");
                    }

                    // 去除前后，
                    String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                    String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());


                    // 返回的数据格式，<userid,partAggrInfo>
                    // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                    // 然后再直接将返回的Tuple的key设置成sessionid
                    // 数据格式，还是<sessionid,fullAggrInfo>
                    // 聚合数据统一定义，使用key=value|key=value
                    partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                            + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                            + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;
                }

                return new Tuple2<>(userid, partAggrInfo);
            }
        });

        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userIdPairRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });

        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> joinRDD = userId_Session_Word_CatagoryRDD.join(userIdPairRDD);

        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> fullRDD = joinRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                String aggrInfo = tuple._2._1;
                Row row = tuple._2._2;


                String sessionid = StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                int age = row.getInt(3);
                String professional = row.getString(4);
                String city = row.getString(5);
                String sex = row.getString(6);

                String fullAggrInfo = aggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<>(sessionid, fullAggrInfo);
            }
        });

        return fullRDD;
    }


    /**
     * 范围过滤
     *
     * @param sqlContext
     * @param taskParamJson
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParamJson) {
        String startDate = ParamUtils.getParam(taskParamJson, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParamJson, Constants.PARAM_END_DATE);

        String sql = "select * "
                + "from user_visit_action "
                + "where date>='" + startDate + "' "
                + "and date<='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();
    }


    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(JavaSparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc.sc());
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

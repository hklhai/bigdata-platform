package com.hxqh.bigdata.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.hxqh.bigdata.conf.ConfigurationManager;
import com.hxqh.bigdata.dao.ISessionAggrStatDAO;
import com.hxqh.bigdata.dao.ISessionDetailDAO;
import com.hxqh.bigdata.dao.ITop10CategoryDAO;
import com.hxqh.bigdata.dao.ITop10SessionDAO;
import com.hxqh.bigdata.dao.factory.DAOFactory;
import com.hxqh.bigdata.domain.SessionAggrStat;
import com.hxqh.bigdata.domain.SessionDetail;
import com.hxqh.bigdata.domain.Top10Category;
import com.hxqh.bigdata.domain.Top10Session;
import com.hxqh.bigdata.jdbc.Constants;
import com.hxqh.bigdata.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Ocean lin on 2017/12/22.
 *
 * @author Ocean lin
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

    private static final Integer TOP_TEN = 3;


    public static void main(String[] args) {
        args = new String[]{"1"};

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(Constants.SPARK_APP_NAME_SESSION);
        // 内存占比
        sparkConf.set("spark.storage.memoryFraction", "0.5")
                // map合并机制
                .set("spark.shuffle.consolidateFiles", "true")
                // map内存缓冲默认大小
                .set("spark.shuffle.file.buffer", "64")
                // reduce内存占比
                .set("spark.shuffle.memoryFraction", "0.3")
                .set("spark.reducer.maxSizeInFlight", "24")
                .set("spark.shuffle.io.maxRetries", "60")
                .set("spark.shuffle.io.retryWait", "60")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");


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

        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

//        JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
//            @Override
//            public Tuple2<String, Row> call(Row row) throws Exception {
//                return new Tuple2<>(row.getString(2), row);
//            }
//        });


        // 改写为mapPartitionsToPair
        JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> rowIterator) throws Exception {

                List<Tuple2<String, Row>> list = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    list.add(new Tuple2<>(row.getString(2), row));
                }
                return list;
            }
        });


        sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());


        JavaPairRDD<String, String> fullRDD = aggByUserId(sqlContext, sessionid2actionRDD);
        System.out.println("fullRDD:" + fullRDD.count());

        // 增加自定义累加器
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filterRDDByParameterRDD = filterRDDByParameterAndAggrStage(taskParam, fullRDD, sessionAggrStatAccumulator);
        System.out.println("filterRDD:" + filterRDDByParameterRDD.count());
        filterRDDByParameterRDD.persist(StorageLevel.MEMORY_ONLY());

        /**
         * 对于Accumulator这种分布式累加计算的变量的使用
         *
         * 从Accumulator中，获取数据，插入数据库的时候，一定要在有某一个action操作以后再进行！！！
         *
         * 如果没有action的话，那么整个程序根本不会运行
         * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take不正确的，需要在该方法之前
         *
         * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
         *
         *
         */


        /**
         * 使用到了比较大的变量，每个task都会拷贝一份副本，
         * 比较消耗内存和网络传输的性能
         */

        // 存在countByKey不需要使用count算子触发执行
        randomExtractSession(filterRDDByParameterRDD, sc);

        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), taskid, prop, sqlContext);


        // 生成公共的RDD：通过筛选条件的session的访问明细数据
        /**
         * 通过筛选生成的访问明细数据
         *
         * 第一选择 StorageLevel.MEMORY_ONLY()
         * 第二选择 StorageLevel.MEMORY_ONLY_SER()
         * 第三选择 StorageLevel.MEMORY_AND_DISK()
         * 第四选择 StorageLevel.MEMORY_AND_DISK_SER()
         * 第五选择 StorageLevel.DISK_ONLY()
         *
         * 内容充足使用双副本高可靠机制
         * StorageLevel.MEMORY_ONLY_2()
         */
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(filterRDDByParameterRDD, sessionid2actionRDD);
        sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());


        // 获取top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(taskid, sessionid2detailRDD, sessionid2actionRDD);


        // 获取top10活跃session
        getTop10Session(sc, taskid, top10CategoryList, sessionid2detailRDD);

        sc.close();
    }


    private static JavaPairRDD<String, Row> getSessionid2detailRDD(JavaPairRDD<String, String> filterRDDByParameter, JavaPairRDD<String, Row> sessionid2actionRDD) {
        // 获取符合条件的session的访问明细
        JavaPairRDD<String, Tuple2<String, Row>> joinRDD = filterRDDByParameter.join(sessionid2actionRDD);
        JavaPairRDD<String, Row> stringRowJavaPairRDD = joinRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1, tuple2._2._2);
            }
        });
        return stringRowJavaPairRDD;
    }


    private static JavaPairRDD<String, String> filterRDDByParameterAndAggrStage(JSONObject taskParam, JavaPairRDD<String, String> fullRDD, final Accumulator<String> sessionAggrStatAccumulator) {
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
        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

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


                /*********************Session 访问时长、访问步长*********************/
                // 经过之前的多个过滤条件之后，那么说明该session是通过了用户指定的筛选条件的，是需要保留的session
                // 那么要对session的访问时长和访问步长，进行统计，根据session对应的范围进行相应的累加计数

                // 提取出该端的步长，然后加1
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);
                /*********************Session 访问时长、访问步长*********************/

                return true;
            }

            /**
             * 计算访问时长范围
             * @param visitLength
             */
            private void calculateVisitLength(long visitLength) {
                if (visitLength >= 1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitLength >= 4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitLength >= 7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitLength >= 10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            /**
             * 计算访问步长范围
             * @param stepLength
             */
            private void calculateStepLength(long stepLength) {
                if (stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if (stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if (stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if (stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if (stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if (stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });
        return filter;
    }


    /**
     * 聚合为<SessionId,SessionId_words_catagorys>形式
     *
     * @param sessionid2actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggByUserId(SQLContext sqlContext, JavaPairRDD<String, Row> sessionid2actionRDD) {

        JavaPairRDD<String, Iterable<Row>> sessionGroupRDD = sessionid2actionRDD.groupByKey();

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

                /*********************Session 访问时长、访问步长*********************/
                // session的起始和结束时间
                Date startTime = null;
                Date endTime = null;
                // session的访问步长
                int stepLength = 0;
                /*********************Session 访问时长、访问步长*********************/

                // 遍历session所有的访问行为
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (userid == null) {
                        userid = row.getLong(1);
                    }

                    String searchKeyword = row.getString(5);
                    Long clickCategoryId = row.getLong(6);

                    // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                    // 只有搜索行为，是有searchKeyword字段的;只有点击品类的行为，是有clickCategoryId字段的
                    // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                    // 是否将搜索词或点击品类id拼接到字符串中去
                    // 首先要满足2个条件：1.不能是null值；2.之前的字符串中还没有搜索词或者点击品类id
                    if (StringUtils.isNotEmpty(searchKeyword)) {
                        if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                            searchKeywordsBuffer.append(searchKeyword + ",");
                        }
                    }
                    if (null != clickCategoryId) {
                        if (!(clickCategoryId.toString().contains(String.valueOf(clickCategoryId)))) {
                            clickCategoryIdsBuffer.append(clickCategoryId + ",");
                        }
                    }

                    // 去除前后，
                    String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                    String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());


                    /*********************Session 访问时长、访问步长*********************/
                    // 计算session开始和结束时间
                    Date actionTime = DateUtils.parseTime(row.getString(4));

                    // 计算开始时间与结束时间
                    if (startTime == null) {
                        startTime = actionTime;
                    }
                    if (endTime == null) {
                        endTime = actionTime;
                    }

                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }

                    // 计算session访问步长
                    stepLength++;

                    // 计算session访问时长（秒）
                    long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
                    /*********************Session 访问时长、访问步长*********************/


                    // 返回的数据格式，<userid,partAggrInfo>
                    // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                    // 然后再直接将返回的Tuple的key设置成sessionid
                    // 数据格式，还是<sessionid,fullAggrInfo>
                    // 聚合数据统一定义，使用key=value|key=value
                    partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                            + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                            + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                            + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                            + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                            + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
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

        String sql_allData = "select * from user_visit_action ";
        DataFrame allDataDF = sqlContext.sql(sql_allData);
        System.out.println(allDataDF.count());

        String startDate = ParamUtils.getParam(taskParamJson, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParamJson, Constants.PARAM_END_DATE);

        String sql = "select * "
                + "from user_visit_action "
                + "where date>='" + startDate + "' "
                + "and date<='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        /**
         * 此处易发生性能问题，
         * 如Spark SQL默认给第一个stage设置了20个task，但是根据根据数据量及算法的复杂度
         * 实际上需要1000个task并行执行
         *
         * 这里可以对Spark SQL刚查询的RDD执行repartition操作
         */
//        return actionDF.javaRDD().repartition(1000);

        System.out.println(actionDF.count());
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

    private static void randomExtractSession(JavaPairRDD<String, String> filterRDDByParameter, JavaSparkContext sc) {
        // 计算每天每小时session数量
        JavaPairRDD<String, String> date_aggrInfo_RDD = filterRDDByParameter.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                String startTime = StringUtils.getFieldFromConcatString(tuple2._2, "\\|", Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(startTime);
                return new Tuple2<>(dateHour, tuple2._2);
            }
        });
        // countByKey
        /**
         *
         * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
         * 首先抽取出的session的聚合数据，写入session_random_extract表
         * 所以第一个RDD的value，应该是session聚合数据
         *
         */

        Map<String, Object> date_aggrInfo_Map = date_aggrInfo_RDD.countByKey();

        // 第二步，按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
        Map<String, Map<String, Long>> dayHourCountMap = new HashMap<>(200);
        Broadcast<Map<String, Map<String, Long>>> dayHourCountMapBroadcast = sc.broadcast(dayHourCountMap);


        for (Map.Entry<String, Object> entry : date_aggrInfo_Map.entrySet()) {
            String dateHour = entry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            Long count = Long.valueOf(String.valueOf(entry.getValue()));
            Map<String, Long> hourCountMap = dayHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dayHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour, count);
        }


        // 按时间比例随机抽取算法
        // 总共要抽取100个session，先按照天数，再进行平分
        // todo 待完成
        //int extractNumberPerDay = 100 / dayHourCountMap.size();

        /**
         * 使用广播变量的时候
         * 直接调用广播变量（Broadcast类型）的value() / getValue()
         * 可以获取到之前封装的广播变量
         */
//        Map<String, Map<String, IntList>> dateHourExtractMap = dayHourCountMapBroadcast.value();


    }


    private static void calculateAndPersistAggrStat(String value, long taskid, Properties prop, SQLContext sqlContext) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        System.out.println(visit_length_1s_3s + "======" + session_count);
        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

        // todo  可以使用构造DataFrame方式
//        DataFrame dataFrame = sqlContext.read().jdbc(ConfigurationManager.getProperty("jdbc.url"),
//                "task", new String[]{"task_id=" + taskid}, prop).select("task_param");


        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }


    private static List<Tuple2<CategorySortKey, String>> getTop10Category(long taskid, JavaPairRDD<String, Row> sessionid2detailRDD, JavaPairRDD<String, Row> sessionid2actionRDD) {
        /**
         * 第一步：获取符合条件的session访问过的所有品类
         */

        // 获取session访问过的所有品类id  指点击过、下单过、支付过的品类
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                Long clickCategoryId = row.getLong(6);
                if (clickCategoryId != null) {
                    list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                }

                String orderCategoryIds = row.getString(8);
                if (orderCategoryIds != null) {
                    String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                    for (String orderCategoryId : orderCategoryIdsSplited) {
                        list.add(new Tuple2<>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                    }
                }

                String payCategoryIds = row.getString(10);
                if (payCategoryIds != null) {
                    String[] payCategoryIdsSplited = payCategoryIds.split(",");
                    for (String payCategoryId : payCategoryIdsSplited) {
                        list.add(new Tuple2<>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
                    }
                }

                return list;
            }
        });

        categoryidRDD = categoryidRDD.distinct();
        /**
         * 第二步：计算各品类的点击、下单和支付的次数
         */
        // 访问明细中，其中三种访问行为是：点击、下单和支付
        // 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
        // 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算

        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD);

        /**
         * 第三步：join各品类与它的点击、下单和支付的次数
         *
         * categoryidRDD中，是包含了所有的符合条件的session，访问过的品类id
         *
         * 已经计算出来的各品类的点击、下单和支付的次数，可能不是包含所有品类的
         * 比如，有的品类，就只是被点击过，但是没有人下单和支付
         *
         * 所以，这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryidRDD不能
         * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryidRDD还是要保留下来的
         * 只不过，没有join到的那个数据，就是0了
         *
         */
        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD,
                orderCategoryId2CountRDD, payCategoryId2CountRDD);


        /**
         * 第四步：自定义二次排序key
         */

        /**
         * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
         */
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = categoryid2countRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
                String countInfo = tuple._2;
                long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
                CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);
                return new Tuple2<>(sortKey, countInfo);
            }
        }).sortByKey(false);

        /**
         * 第六步：用take(10)取出top10热门品类，并写入MySQL
         */
        List<Tuple2<CategorySortKey, String>> top10RDD = sortedCategoryCountRDD.take(10);

        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();

        for (Tuple2<CategorySortKey, String> tuple : top10RDD) {
            String countInfo = tuple._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
            Top10Category category = new Top10Category(taskid, categoryid, clickCount, orderCount, payCount);
            top10CategoryDAO.insert(category);
        }

        return top10RDD;
    }

    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> stringRowJavaPairRDD) {

        /**
         * 采用coalesce压缩数据量到100
         *
         * local模式不需要设置coalesce,local本地模式采用在本地线程模拟partition，本地模式设置coalesce无效
         *
         *
         */
        JavaPairRDD<String, Row> clickRDD = stringRowJavaPairRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Row row = v1._2;
                return row.get(6) != null ? true : false;
            }
        }).coalesce(100);


        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                Long clickCategoryId = tuple._2.getLong(6);
                return new Tuple2<>(clickCategoryId, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return clickCategoryId2CountRDD;
    }

    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> stringRowJavaPairRDD) {

        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = stringRowJavaPairRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Row row = v1._2;
                return row.getString(8) != null ? true : false;
            }
        }).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                Row row = tuple2._2;
                String orderCategoryIds = row.getString(8);
                String[] split = orderCategoryIds.split(",");
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                for (String s : split) {
                    list.add(new Tuple2<>(Long.valueOf(s), 1L));
                }
                return list;
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return orderCategoryId2CountRDD;
    }

    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> stringRowJavaPairRDD) {
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = stringRowJavaPairRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Row row = v1._2;
                return row.getString(10) != null ? true : false;
            }
        }).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                Row row = tuple2._2;
                String payCategoryIds = row.getString(10);
                String[] split = payCategoryIds.split(",");
                List<Tuple2<Long, Long>> list = new ArrayList<>();

                for (String s : split) {
                    list.add(new Tuple2<>(Long.valueOf(s), 1L));
                }
                return list;
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return payCategoryId2CountRDD;
    }

    /**
     * 连接品类RDD与数据RDD
     *
     * @param categoryidRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long, String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryidRDD, JavaPairRDD<Long, Long> clickCategoryId2CountRDD, JavaPairRDD<Long, Long> orderCategoryId2CountRDD, JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        // 如果用leftOuterJoin，就可能出现，右边那个RDD中，join过来时，没有值
        // 所以Tuple中的第二个值用Optional<Long>类型，就代表，可能有值，可能没有值

        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);
        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple2) throws Exception {
                long categoryid = tuple2._1;
                Optional<Long> optional = tuple2._2._2;
                long clickCount = 0L;
                if (optional.isPresent()) {
                    clickCount = optional.get();
                }
                String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" +
                        Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                return new Tuple2<>(categoryid, value);
            }
        });


        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                long categoryid = tuple._1;
                String value = tuple._2._1;

                Optional<Long> optional = tuple._2._2;
                long orderCount = 0L;

                if (optional.isPresent()) {
                    orderCount = optional.get();
                }

                value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                return new Tuple2<>(categoryid, value);
            }
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                long categoryid = tuple._1;
                String value = tuple._2._1;
                Optional<Long> optional = tuple._2._2;
                long payCount = 0L;

                if (optional.isPresent()) {
                    payCount = optional.get();
                }
                value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
                return new Tuple2<>(categoryid, value);
            }
        });

        return tmpMapRDD;
    }

    private static void getTop10Session(JavaSparkContext sc, final long taskId, List<Tuple2<CategorySortKey, String>> top10CategoryList, JavaPairRDD<String, Row> sessionid2detailRDD) {

        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<>();
        for (Tuple2<CategorySortKey, String> tuple2List : top10CategoryList) {
            String category = tuple2List._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(category, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<>(categoryid, categoryid));
        }
        JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelize(top10CategoryIdList).
                mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<Long, Long> longLongTuple2) throws Exception {
                        return new Tuple2<>(longLongTuple2._1, longLongTuple2._2);
                    }
                });

        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = sessionid2detailRDD.groupByKey();


        JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.
                flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                        String sessionid = tuple2._1;
                        Iterator<Row> iterator = tuple2._2.iterator();

                        Map<Long, Long> categoryCountMap = new HashMap<>();
                        // 计算出该session，对每个品类的点击次数
                        while (iterator.hasNext()) {
                            Row row = iterator.next();

                            if (row.get(6) != null) {
                                long categoryid = row.getLong(6);

                                Long count = categoryCountMap.get(categoryid);
                                if (count == null) {
                                    count = 0L;
                                }
                                count++;
                                categoryCountMap.put(categoryid, count);
                            }
                        }

                        // 返回结果，<(categoryid,(sessionid,count))>格式
                        List<Tuple2<Long, String>> list = new ArrayList<>();

                        for (Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                            long categoryid = categoryCountEntry.getKey();
                            long count = categoryCountEntry.getValue();
                            String value = sessionid + "," + count;
                            list.add(new Tuple2<>(categoryid, value));
                        }

                        return list;
                    }
                });

        // 获取到to10热门品类，被各个session点击的次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD).
                mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                        return new Tuple2<>(tuple._1, tuple._2._2);
                    }
                });

        /**
         * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey();


        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple2) throws Exception {
                long categoryid = tuple2._1;
                Iterator<String> iterator = tuple2._2.iterator();

                // 定义取topn的排序数组
                String[] top10Sessions = new String[TOP_TEN];

                while (iterator.hasNext()) {
                    String sessionCount = iterator.next();
                    long count = Long.valueOf(sessionCount.split(",")[1]);

                    // 每次均遍历排序数组
                    for (int i = 0; i < top10Sessions.length; i++) {
                        // 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
                        if (top10Sessions[i] == null) {
                            top10Sessions[i] = sessionCount;
                            break;
                        } else {
                            long countTmp = Long.valueOf(top10Sessions[i].split(",")[1]);

                            // 如果sessionCount比i位的sessionCount要大
                            if (count > countTmp) {
                                // 从排序数组最后一位开始，到i位，所有数据往后挪一位
                                for (int j = TOP_TEN - 1; j > i; j--) {
                                    top10Sessions[j] = top10Sessions[j - 1];
                                }
                                // 将i位赋值为sessionCount
                                top10Sessions[i] = sessionCount;
                                break;
                            }
                        }
                    }
                }

                // 将数据写入MySQL表
                List<Tuple2<String, String>> list = new ArrayList<>();

                for (String sessionCount : top10Sessions) {
                    if (sessionCount != null) {
                        String sessionid = sessionCount.split(",")[0];
                        long count = Long.valueOf(sessionCount.split(",")[1]);

                        // 将top10 session插入MySQL表
                        Top10Session top10Session = new Top10Session();
                        top10Session.setTaskid(taskId);
                        top10Session.setCategoryid(categoryid);
                        top10Session.setSessionid(sessionid);
                        top10Session.setClickCount(count);

                        ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                        top10SessionDAO.insert(top10Session);

                        // 放入list
                        list.add(new Tuple2<>(sessionid, sessionid));
                    }
                }
                return list;
            }
        });

        /**
         * 第四步：获取top10活跃session的明细数据，并写入MySQL
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD = top10SessionRDD.join(sessionid2detailRDD);

        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
                Row row = tuple2._2._2;

                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskId);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });


    }
}

package com.hxqh.bigdata.spark.ad;

import com.google.common.base.Optional;
import com.hxqh.bigdata.common.Constants;
import com.hxqh.bigdata.conf.ConfigurationManager;
import com.hxqh.bigdata.dao.IAdBlacklistDAO;
import com.hxqh.bigdata.dao.IAdStatDAO;
import com.hxqh.bigdata.dao.IAdUserClickCountDAO;
import com.hxqh.bigdata.dao.factory.DaoFactory;
import com.hxqh.bigdata.domain.AdBlacklist;
import com.hxqh.bigdata.domain.AdStat;
import com.hxqh.bigdata.domain.AdUserClickCount;
import com.hxqh.bigdata.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Ocean lin on 2018/2/24.
 * 广告点击流量实时统计spark作业
 *
 * @author Ocean lin
 */
public class AdClickRealTimeStatSpark {


    private final static int NUM = 100;

    public static void main(String[] args) {
        // 1. 上下文
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName(Constants.SPARK_APP_NAME_AD);

        // spark streaming的上下文是构建JavaStreamingContext对象
        // 而不是像之前的JavaSparkContext、SQLContext/HiveContext
        // 传入的第一个参数，和之前的spark上下文一样，也是SparkConf对象；第二个参数则不太一样

        // 第二个参数是spark streaming类型作业比较有特色的一个参数
        // 实时处理batch的interval
        // spark streaming，每隔一小段时间，会去收集一次数据源（kafka）中的数据，做成一个batch
        // 每次都是处理一个batch中的数据

        // 通常来说，batch interval，就是指每隔多少时间收集一次数据源中的数据，然后进行处理
        // 一遍spark streaming的应用，都是设置数秒到数十秒（很少会超过1分钟）

        // 咱们这里项目中，就设置5秒钟的batch interval
        // 每隔5秒钟，咱们的spark streaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));


        // 正式开始进行代码的编写
        // 实现咱们需要的实时计算的业务逻辑和功能

        // 创建针对Kafka数据来源的输入DStream（离线流，代表了一个源源不断的数据来源，抽象）
        // 选用kafka direct api（很多好处，包括kafka内部自适应调整每次接收数据量的特性，等等）

        // 构建kafka参数map
        // 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<>();
        // KafkaUtils要求传入set
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }

        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);


        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream);

        // 生成动态黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);


        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount） (最粗)
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream);


        // 业务功能二：实时统计每天每个省份top3热门广告 (中)


        // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量） （精细）



        // 构建完spark streaming上下文之后，记得要进行上下文的启动、等待执行结束、关闭
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();
    }


    private static JavaPairDStream<String, String> filterByBlacklist(
            JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // 刚刚接受到原始的用户点击行为日志之后
        // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
        // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能强大）
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    private static final long serialVersionUID = 1L;

                    @SuppressWarnings("resource")
                    @Override
                    public JavaPairRDD<String, String> call(
                            JavaPairRDD<String, String> rdd) throws Exception {

                        // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
                        IAdBlacklistDAO adBlacklistDAO = DaoFactory.getAdBlacklistDAO();
                        List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();

                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<>();

                        for (AdBlacklist adBlacklist : adBlacklists) {
                            tuples.add(new Tuple2<>(adBlacklist.getUserid(), true));
                        }

                        JavaSparkContext sc = new JavaSparkContext(rdd.context());
                        JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);

                        // 将原始数据rdd映射成<userid, tuple2<string, string>>
                        JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                                String log = tuple._2;
                                String[] logSplited = log.split(" ");
                                long userid = Long.valueOf(logSplited[3]);
                                return new Tuple2<>(userid, tuple);
                            }

                        });

                        // 将原始日志数据rdd，与黑名单rdd，进行左外连接
                        // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
                        // 用inner join，内连接，会导致数据丢失
                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD);

                        // 过滤出为null的用户就是白名单用户
                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
                                new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Boolean call(
                                            Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                        Optional<Boolean> optional = tuple._2._2;

                                        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
                                        if (optional.isPresent() && optional.get()) {
                                            return false;
                                        }
                                        return true;
                                    }

                                });

                        JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
                                new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Tuple2<String, String> call(
                                            Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                        return tuple._2._1;
                                    }
                                });

                        return resultRDD;
                    }

                });

        return filteredAdRealTimeLogDStream;
    }


    /**
     * 生成动态黑名单
     *
     * @param filteredAdRealTimeLogDStream
     */
    private static void generateDynamicBlacklist(
            JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 一条一条的实时日志
        // timestamp province city userid adid
        // 某个时间点 某个省份 某个城市 某个用户 某个广告

        // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量

        // 通过对原始实时日志的处理
        // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        // 从tuple中获取到每一条原始的实时日志
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        // 提取出日期（yyyyMMdd）、userid、adid
                        String timestamp = logSplited[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String datekey = DateUtils.formatDateKey(date);

                        long userid = Long.valueOf(logSplited[3]);
                        long adid = Long.valueOf(logSplited[4]);

                        // 拼接key
                        String key = datekey + "_" + userid + "_" + adid;

                        return new Tuple2<>(key, 1L);
                    }

                });

        // 针对处理后的日志格式，执行reduceByKey算子即可
        // （每个batch中）每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(
                new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        // 到这里为止，获取到了什么数据呢？
        // dailyUserAdClickCountDStream DStream
        // 源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
        // <yyyyMMdd_userid_adid, clickCount>
        dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        // 对每个分区的数据就去获取一次连接对象
                        // 每次都是从连接池中获取，而不是每次都创建
                        // 写数据库操作，性能已经提到最高了

                        List<AdUserClickCount> adUserClickCounts = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();

                            String[] keySplited = tuple._1.split("_");
                            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                            // yyyy-MM-dd
                            long userId = Long.valueOf(keySplited[1]);
                            long adId = Long.valueOf(keySplited[2]);
                            long clickCount = tuple._2;

                            AdUserClickCount adUserClickCount = new AdUserClickCount(date, userId, adId, clickCount);

                            adUserClickCounts.add(adUserClickCount);
                        }

                        IAdUserClickCountDAO adUserClickCountDAO = DaoFactory.getAdUserClickCountDAO();
                        adUserClickCountDAO.updateBatch(adUserClickCounts);
                    }
                });
                return null;
            }
        });

        // 现在mysql里面，已经有了累计的每天各用户对各广告的点击量
        // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
        // 从mysql中查询
        // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
        // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化

        // 对batch中的数据，去查询mysql中的点击次数，使用dstream即dailyUserAdClickCountDStream
        // 因为这个batch是聚合过的数据，已经按照yyyyMMdd_userid_adid进行过聚合了
        // 比如原始数据可能是一个batch有一万条，聚合过后可能只有五千条
        // 所以选用这个聚合后的dstream，既可以满足咱们的需求，而且呢，还可以尽量减少要处理的数据量,一举两得

        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(new Function<Tuple2<String, Long>, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                String key = tuple._1;
                String[] keySplited = key.split("_");

                // yyyyMMdd -> yyyy-MM-dd
                String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                long userId = Long.valueOf(keySplited[1]);
                long adId = Long.valueOf(keySplited[2]);

                // 从mysql中查询指定日期指定用户对指定广告的点击量
                IAdUserClickCountDAO adUserClickCountDAO = DaoFactory.getAdUserClickCountDAO();
                int clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userId, adId);

                // 判断，如果点击量大于等于100，ok，那么不好意思，你就是黑名单用户
                // 那么就拉入黑名单，返回true
                if (clickCount >= NUM) {
                    return true;
                }

                // 反之，如果点击量小于100的，那么就暂时不要管它了
                return false;
            }

        });

        // blacklistDStream
        // 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
        // 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
        // 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
        // 可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
        // 所以直接插入mysql即可

        // blacklistDStream中，可能有userId是重复的，如果直接这样插入的话
        // 那么是不是会发生，插入重复的黑明单用户
        // 我们在插入前要进行去重
        // yyyyMMdd_userid_adid
        // 20151220_10001_10002 100
        // 20151220_10001_10003 100
        // 10001这个userid就重复了

        // 实际上，是要通过对dstream执行操作，对其中的rdd中的userId进行全局的去重
        JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(
                new Function<Tuple2<String, Long>, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Tuple2<String, Long> tuple) throws Exception {
                        String key = tuple._1;
                        String[] keySplited = key.split("_");
                        Long userid = Long.valueOf(keySplited[1]);
                        return userid;
                    }

                });

        JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                return rdd.distinct();
            }

        });

        // 到这一步为止，distinctBlacklistUseridDStream
        // 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的
        distinctBlacklistUseridDStream.foreachRDD(new Function<JavaRDD<Long>, Void>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Void call(JavaRDD<Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Long> iterator) throws Exception {
                        List<AdBlacklist> adBlacklists = new ArrayList<>();

                        while (iterator.hasNext()) {
                            long userid = iterator.next();

                            AdBlacklist adBlacklist = new AdBlacklist();
                            adBlacklist.setUserid(userid);

                            adBlacklists.add(adBlacklist);
                        }

                        IAdBlacklistDAO adBlacklistDAO = DaoFactory.getAdBlacklistDAO();
                        adBlacklistDAO.insertBatch(adBlacklists);

                        // 完成动态黑名单

                        // 1、计算出每个batch中的每天每个用户对每个广告的点击量，并持久化到mysql中

                        // 2、依据上述计算出来的数据，对每个batch中的按date、userId、adId聚合的数据
                        // 都要遍历一遍，查询一下，对应的累计的点击次数，如果超过了100，那么就认定为黑名单
                        // 然后对黑名单用户进行去重，去重后，将黑名单用户，持久化插入到mysql中
                        // mysql中的ad_blacklist表中的黑名单用户，就是动态地实时地增长的
                        // mysql中的ad_blacklist表，就可以认为是一张动态黑名单表

                        // 3、基于上述计算出来的动态黑名单，在最一开始，就对每个batch中的点击行为
                        // 根据动态黑名单进行过滤
                        // 把黑名单中的用户的点击行为，直接过滤掉
                    }

                });

                return null;
            }

        });
    }


    /**
     * 计算广告点击流量实时统计
     *
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 业务逻辑一
        // 广告点击流量实时统计
        // 黑名单实际上是广告类的实时系统中，比较常见的一种基础的应用

        // 计算每天各省各城市各广告的点击量
        // 数据实时不断地更新到mysql中的，J2EE系统，是提供实时报表给用户查看的
        // j2ee系统每隔几秒钟，就从mysql中搂一次最新数据，每次都可能不一样
        // 设计出来几个维度：日期、省份、城市、广告
        // 用户可以看到，实时的数据，比如2015-11-01，历史数据
        // 2015-12-01，当天，可以看到当天所有的实时数据（动态改变），比如江苏省南京市
        // 广告可以进行选择（广告主、广告名称、广告类型来筛选一个出来）
        // 拿着date、province、city、adid，去mysql中查询最新的数据
        // 等等，基于这几个维度，以及这份动态改变的数据，是可以实现比较灵活的广告点击流量查看的功能的

        // date province city userid adid
        // date_province_city_adid，作为key；1作为value
        // 通过spark，直接统计出来全局的点击次数，在spark集群中保留一份；在mysql中，也保留一份
        // 我们要对原始数据进行map，映射成<date_province_city_adid,1>格式
        // 然后呢，对上述格式的数据，执行updateStateByKey算子
        // spark streaming特有的一种算子，在spark集群内存中，维护一份key的全局状态
        JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        String timeStamp = logSplited[0];
                        Date date = new Date(Long.valueOf(timeStamp));
                        // yyyyMMdd
                        String dateKey = DateUtils.formatDateKey(date);

                        String province = logSplited[1];
                        String city = logSplited[2];
                        long adId = Long.valueOf(logSplited[4]);

                        String key = dateKey + "_" + province + "_" + city + "_" + adId;

                        return new Tuple2<>(key, 1L);
                    }

                });

        // 在这个dstream中，就相当于，有每个batch rdd累加的各个key（各天各省份各城市各广告的点击次数）
        // 每次计算出最新的值，就在aggregatedDStream中的每个batch rdd中反应出来
        JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey(
                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
                        // 举例来说
                        // 对于每个key，都会调用一次这个方法
                        // 比如key是<20151201_Jiangsu_Nanjing_10001,1>，就会来调用一次这个方法7
                        // 10个

                        // values，(1,1,1,1,1,1,1,1,1,1)

                        // 首先根据optional判断，之前这个key，是否有对应的状态
                        long clickCount = 0L;

                        // 如果说，之前是存在这个状态的，那么就以之前的状态作为起点，进行值的累加
                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        // values，代表了，batch rdd中，每个key对应的所有的值
                        for (Long value : values) {
                            clickCount += value;
                        }

                        return Optional.of(clickCount);
                    }
                });

        // 将计算出来的最新结果，同步一份到mysql中给J2EE系统使用
        aggregatedDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdStat> adStatList = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();

                            String[] keySplited = tuple._1.split("_");
                            String date = keySplited[0];
                            String province = keySplited[1];
                            String city = keySplited[2];
                            long adId = Long.valueOf(keySplited[3]);

                            long clickCount = tuple._2;

                            AdStat adStat = new AdStat(date, province, city, adId, clickCount);
                            adStatList.add(adStat);
                        }

                        IAdStatDAO adStatDAO = DaoFactory.getAdStatDAO();
                        adStatDAO.updateBatch(adStatList);
                    }
                });

                return null;
            }

        });

        return aggregatedDStream;
    }


}

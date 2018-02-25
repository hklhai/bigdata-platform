package com.hxqh.bigdata.spark.ad;

/**
 * Created by Ocean lin on 2018/2/25.
 * 广告点击流量实时统计spark作业 高可用
 *
 * @author Ocean lin
 */
public class AdClickRealTimeStatSparkHA {


//    private final static String CHECKPOINTDIRECTORY = "hdfs:spark02:9000/streaming_checkpoint_driver_ha";
//
//    public static void main(String[] args) {
//        // 1. 上下文
//        JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
//            @Override
//            public JavaStreamingContext create() {
//                SparkConf sparkConf = new SparkConf().setAppName("AdClickRealTimeStatSparkHA");
//                // WAL（Write-Ahead Log）预写日志机制
//                sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
//                JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
//
//                Map<String, String> kafkaParams = new HashMap<>();
//                kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST, ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
//
//                // 构建topic set
//                String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
//                String[] kafkaTopicsSplited = kafkaTopics.split(",");
//                Set<String> topics = new HashSet<>();
//                // KafkaUtils要求传入set
//                for (String kafkaTopic : kafkaTopicsSplited) {
//                    topics.add(kafkaTopic);
//                }
//
//                JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
//                        jssc,
//                        String.class,
//                        String.class,
//                        StringDecoder.class,
//                        StringDecoder.class,
//                        kafkaParams,
//                        topics);
//
//                // kafka方式获取
//                // 处理adRealTimeLogDStream
//
//
//                // socket模拟数据方式
//                // JavaDStream<String> lines = jssc.socketTextStream(...);
//
//                jssc.checkpoint(CHECKPOINTDIRECTORY);
//                return jssc;
//            }
//        };
//
//        JavaStreamingContext context = JavaStreamingContext.getOrCreate(CHECKPOINTDIRECTORY, contextFactory);
//        context.start();
//        context.awaitTermination();
//    }


}

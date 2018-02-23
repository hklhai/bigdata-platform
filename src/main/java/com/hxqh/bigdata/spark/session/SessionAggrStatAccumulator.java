package com.hxqh.bigdata.spark.session;

import com.hxqh.bigdata.common.Constants;
import com.hxqh.bigdata.util.StringUtils;
import org.apache.spark.AccumulatorParam;

import java.io.Serializable;

/**
 * Created by Ocean lin on 2017/12/26.
 * <p>
 * <p>
 * 使用自定义的一些数据格式，比如String，也可以自己定义model，自定义的类（必须可序列化）
 * 然后可以基于这种特殊的数据格式，可以实现自己复杂的分布式的计算逻辑
 * 各个task，分布式在运行，可以根据需求，task给Accumulator传入不同的值
 * 根据不同的值，做复杂的逻辑
 * <p>
 * <p>
 * AccumulatorParam<String>表示针对什么格式的数据进行分布式计算
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String>, Serializable {

    private static final long serialVersionUID = 6311074555136039130L;

    /**
     * zero方法，主要用于数据的初始化
     * 返回一个值，就是初始化中，所有范围区间的数量都是0
     * 各个范围区间的统计数量的拼接，采用的key=value|key=value的连接串的格式
     */
    @Override
    public String zero(String v) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }


    /**
     * addInPlace和addAccumulator是一样的
     * <p>
     * 这两个方法，其实主要就是实现，v1是初始化的那个连接串
     * v2，就是在遍历session的时候，判断出某个session对应的区间，然后会用Constants.TIME_PERIOD_1s_3s
     * 在v1中，找到v2对应的value，累加1，然后再更新回连接串里
     */
    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }


    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }


    /**
     * session统计计算逻辑
     *
     * @param v1 连接串
     * @param v2 范围区间
     * @return 更新以后的连接串
     */
    private String add(String v1, String v2) {
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }
        // 从v1中提取v2对应的值，并累加
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) {
            int newValue = Integer.valueOf(oldValue) + 1;
            // 将v1中v2对应的值设置为累加的值
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
        return v1;
    }
}

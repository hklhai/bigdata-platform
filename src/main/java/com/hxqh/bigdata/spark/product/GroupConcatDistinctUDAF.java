package com.hxqh.bigdata.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * Created by Ocean lin on 2018/2/24.
 * 组内拼接去重函数（group_concat_distinct()）
 * <p>
 * 技术点4：自定义UDAF聚合函数
 *
 * @author Ocean lin
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {


    private static final String SPLIT_LABEL = ",";

    /**
     * 指定输入数据的字段与类型
     */
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));

    /**
     * 指定缓冲数据的字段与类型
     * 维护拼接的中间状态
     */
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));

    /**
     * 指定返回类型
     */
    private DataType dataType = DataTypes.StringType;

    /**
     * 指定是否是确定性的
     * 缺省
     */
    private boolean deterministic = true;

    @Override
    public StructType inputSchema() {
        return null;
    }

    @Override
    public StructType bufferSchema() {
        return null;
    }

    @Override
    public DataType dataType() {
        return null;
    }

    @Override
    public boolean deterministic() {
        return false;
    }


    /**
     * 初始化
     * 可以认为是，你自己在内部指定一个初始的值
     *
     * @param buffer
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        // 初始化为空字符串
        buffer.update(0, "");
    }


    /**
     * 更新 针对一个分组内的部分数据，在某个节点上发生的
     * <p>
     * 一个一个地将组内的字段值传递进来, 实现拼接的逻辑
     *
     * @param buffer
     * @param input
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // 缓冲中的已经拼接过的城市信息串
        String bufferCityInfo = buffer.getString(0);
        // 刚刚传递进来的某个城市信息
        String cityInfo = input.getString(0);

        // 在这里要实现去重的逻辑
        // 判断：之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息
        if (!bufferCityInfo.contains(cityInfo)) {
            if ("".equals(bufferCityInfo)) {
                bufferCityInfo += cityInfo;
            } else {
                // 比如1:北京
                // 1:北京,2:上海
                bufferCityInfo += "," + cityInfo;
            }
            buffer.update(0, bufferCityInfo);
        }
    }


    /**
     * 合并
     * <p>
     * update操作，可能是针对一个分组内的部分数据，在某个节点上发生的
     * 但是可能一个分组内的数据，会分布在多个节点上处理
     * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
     *
     * @param buffer1
     * @param buffer2
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);

        for (String cityInfo : bufferCityInfo2.split(SPLIT_LABEL)) {
            if (!bufferCityInfo1.contains(cityInfo)) {
                if ("".equals(bufferCityInfo1)) {
                    bufferCityInfo1 += cityInfo;
                } else {
                    bufferCityInfo1 += "," + cityInfo;
                }
            }
        }
        buffer1.update(0, bufferCityInfo1);
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}

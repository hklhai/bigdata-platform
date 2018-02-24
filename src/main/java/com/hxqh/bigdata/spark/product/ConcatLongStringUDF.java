package com.hxqh.bigdata.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * Created by Ocean lin on 2018/2/24.
 * 将两个字段拼接起来（使用指定的分隔符）
 *
 * @author Ocean lin
 */
public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {
    @Override
    public String call(Long aLong, String s, String splitLabel) throws Exception {
        return String.valueOf(aLong) + splitLabel + s;
    }
}

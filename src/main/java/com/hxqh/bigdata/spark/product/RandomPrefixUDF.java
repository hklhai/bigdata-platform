package com.hxqh.bigdata.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * Created by Ocean lin on 2018/2/24.
 * random_prefix() 随机前缀
 *
 * @author Ocean lin
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {
    @Override
    public String call(String val, Integer num) throws Exception {
        Random random = new Random();
        int randNum = random.nextInt(10);
        return randNum + "_" + val;
    }
}

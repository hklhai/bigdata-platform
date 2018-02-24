package com.hxqh.bigdata.spark.product;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by Ocean lin on 2018/2/24.
 * 去除随机前缀
 *
 * @author Ocean lin
 */
public class RemoveRandomPrefixUDF implements UDF1<String, String> {
    @Override
    public String call(String val) throws Exception {
        String[] valSplited = val.split("_");
        return valSplited[1];
    }
}

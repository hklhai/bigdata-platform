package com.hxqh.bigdata.jdbc;

/**
 * 常量接口
 */
public interface Constants {

    /**
     * 数据库相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";


    /**
     * 项目配置相关的常量
     */
    String SPARK_LOCAL = "spark.local";



    /**
     * Spark作业相关的常量
     */
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalysis";

}

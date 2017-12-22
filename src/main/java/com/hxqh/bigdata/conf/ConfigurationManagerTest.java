package com.hxqh.bigdata.conf;

/**
 * 配置管理组件测试类
 */
public class ConfigurationManagerTest {

    public static void main(String[] args) {
        String testkey1 = ConfigurationManager.getProperty("jdbc.url.prod");
        String testkey2 = ConfigurationManager.getProperty("jdbc.user.prod");
        System.out.println(testkey1);
        System.out.println(testkey2);
    }

}

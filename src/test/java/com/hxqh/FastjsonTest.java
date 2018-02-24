package com.hxqh;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Ocean lin on 2017/12/22.
 */
public class FastjsonTest {

    @Test
    public void fastjsontest() {

        String json = "[{'学生':'张三', '班级':'一班', '年级':'大一', '科目':'高数', '成绩':90}, {'学生':'李四', '班级':'二班', '年级':'大一', '科目':'高数', '成绩':80}]";
        JSONArray jsonArray = JSONArray.parseArray(json);
        JSONObject jsonObject = jsonArray.getJSONObject(0);
        Assert.assertTrue(jsonObject.get("学生").equals("张三"));
    }

    @Test
    public void testSOH() {
        String s = "0\u0001user0\u0001name0\u000119\u0001professional44\u0001city96\u0001male\n";
        System.out.println(s);
    }

}

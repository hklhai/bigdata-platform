package com.hxqh.bigdata.dao;

import com.hxqh.bigdata.domain.Top10Session;
import com.hxqh.bigdata.jdbc.JDBCHelper;

/**
 * 活跃session top10的DAO实现
 * Created by Ocean lin on 2018/1/17.
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {
    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_session values(?,?,?,?)";

        Object[] params = new Object[]{top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}

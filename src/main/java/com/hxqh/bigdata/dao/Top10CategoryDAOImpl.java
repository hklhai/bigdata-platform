package com.hxqh.bigdata.dao;


import com.hxqh.bigdata.domain.Top10Category;
import com.hxqh.bigdata.jdbc.JDBCHelper;

/**
 * top10品类DAO实现
 *
 * @author Lin
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

    @Override
    public void insert(Top10Category category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}

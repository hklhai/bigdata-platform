package com.hxqh.bigdata.dao;


import com.hxqh.bigdata.domain.PageSplitConvertRate;
import com.hxqh.bigdata.jdbc.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 *
 * @author Lin
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {

    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_convert_rate values(?,?)";
        Object[] params = new Object[]{pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}

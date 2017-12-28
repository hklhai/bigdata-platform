package com.hxqh.bigdata.dao;


import com.hxqh.bigdata.domain.SessionAggrStat;

/**
 * session聚合统计模块DAO接口
 */
public interface ISessionAggrStatDAO {

    /**
     * 插入session聚合统计结果
     *
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);

}

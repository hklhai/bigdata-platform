package com.hxqh.bigdata.dao;


import com.hxqh.bigdata.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 *
 * @author Lin
 */
public interface ISessionRandomExtractDAO {

    /**
     * 插入session随机抽取
     *
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);

}

package com.hxqh.bigdata.dao;

import com.hxqh.bigdata.domain.SessionDetail;

import java.util.List;

/**
 * Created by Ocean lin on 2018/1/18.
 *
 * @author Lin
 */
public interface ISessionDetailDAO {

    /**
     * 插入一条session明细数据
     *
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);

    /**
     * 批量插入session明细数据
     *
     * @param sessionDetails
     */
    void insertBatch(List<SessionDetail> sessionDetails);
}

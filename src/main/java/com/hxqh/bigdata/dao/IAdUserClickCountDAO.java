package com.hxqh.bigdata.dao;

import com.hxqh.bigdata.domain.AdUserClickCount;

import java.util.List;

/**
 * 用户广告点击量DAO接口
 *
 * @author Lin
 */
public interface IAdUserClickCountDAO {

    void updateBatch(List<AdUserClickCount> adUserClickCounts);

}

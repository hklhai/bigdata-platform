package com.hxqh.bigdata.dao;

import com.hxqh.bigdata.domain.AdStat;

import java.util.List;

/**
 * 广告实时统计DAO接口
 *
 * @author Lin
 */
public interface IAdStatDAO {

    void updateBatch(List<AdStat> adStats);

}

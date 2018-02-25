package com.hxqh.bigdata.dao;

import com.hxqh.bigdata.domain.AdClickTrend;

import java.util.List;

/**
 * 广告点击趋势DAO接口
 *
 * @author Lin
 */
public interface IAdClickTrendDAO {

    void updateBatch(List<AdClickTrend> adClickTrends);
}

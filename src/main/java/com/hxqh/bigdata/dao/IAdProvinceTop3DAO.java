package com.hxqh.bigdata.dao;

import com.hxqh.bigdata.domain.AdProvinceTop3;

import java.util.List;

/**
 * 各省份top3热门广告DAO接口
 *
 * @author Lin
 */
public interface IAdProvinceTop3DAO {

    void updateBatch(List<AdProvinceTop3> adProvinceTop3s);

}

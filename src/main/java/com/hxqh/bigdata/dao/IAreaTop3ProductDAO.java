package com.hxqh.bigdata.dao;

import com.hxqh.bigdata.domain.AreaTop3Product;

import java.util.List;

/**
 * 各区域top3热门商品DAO接口
 *
 * @author Lin
 */
public interface IAreaTop3ProductDAO {

    void insertBatch(List<AreaTop3Product> areaTopsProducts);

}

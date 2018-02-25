package com.hxqh.bigdata.dao.factory;


import com.hxqh.bigdata.dao.*;

/**
 * DAO工厂类
 *
 * @author Lin
 */
public class DaoFactory {


    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDAOImpl();
    }

    public static ITop10SessionDAO getTop10SessionDAO() {
        return new Top10SessionDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO() {
        return new SessionDetailDAOImpl();
    }

    public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
        return new PageSplitConvertRateDAOImpl();
    }

    public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
        return new AreaTop3ProductDAOImpl();
    }

    public static IAdUserClickCountDAO getAdUserClickCountDAO() {
        return new AdUserClickCountDAOImpl();
    }

    public static IAdBlacklistDAO getAdBlacklistDAO() {
        return new AdBlacklistDAOImpl();
    }

    public static IAdStatDAO getAdStatDAO() {
        return new AdStatDAOImpl();
    }

    public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
        return new AdProvinceTop3DAOImpl();
    }

    public static IAdClickTrendDAO getAdClickTrendDAO() {
        return new AdClickTrendDAOImpl();
    }
}

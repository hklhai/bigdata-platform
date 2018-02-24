package com.hxqh.bigdata.dao.factory;


import com.hxqh.bigdata.dao.*;

/**
 * DAO工厂类
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


}

package com.hxqh.bigdata.dao.factory;


import com.hxqh.bigdata.dao.ISessionAggrStatDAO;
import com.hxqh.bigdata.dao.ITop10CategoryDAO;
import com.hxqh.bigdata.dao.SessionAggrStatDAOImpl;
import com.hxqh.bigdata.dao.Top10CategoryDAOImpl;

/**
 * DAO工厂类
 *
 */
public class DAOFactory {


    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDAOImpl();
    }

}

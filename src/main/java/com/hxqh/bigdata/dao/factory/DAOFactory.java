package com.hxqh.bigdata.dao.factory;


import com.hxqh.bigdata.dao.ISessionAggrStatDAO;
import com.hxqh.bigdata.dao.SessionAggrStatDAOImpl;

/**
 * DAO工厂类
 *
 * @author Administrator
 */
public class DAOFactory {


    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

}

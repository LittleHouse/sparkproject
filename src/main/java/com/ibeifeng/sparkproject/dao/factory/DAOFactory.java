package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.dao.impl.ITaskDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.SessionDetailDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.Top10CategoryDAOImpl;

public class DAOFactory {
	
	public static ITaskDAO getTaskDAO(){
		return new ITaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}

	public static ISessionDetailDAO getSessionDetailDAO(){
		return new SessionDetailDAOImpl();
	}

	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
}

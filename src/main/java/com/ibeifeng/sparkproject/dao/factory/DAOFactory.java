package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.impl.ITaskDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.SessionAggrStatDAOImpl;

public class DAOFactory {
	
	public static ITaskDAO getTaskDAO(){
		return new ITaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}

}

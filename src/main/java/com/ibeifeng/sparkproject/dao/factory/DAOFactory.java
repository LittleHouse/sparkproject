package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.impl.ITaskDAOImpl;

public class DAOFactory {
	
	public static ITaskDAO getTaskDAO(){
		return new ITaskDAOImpl();
	}

}

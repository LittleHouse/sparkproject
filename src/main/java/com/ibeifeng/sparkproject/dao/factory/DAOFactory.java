package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.IAdBlacklistDAO;
import com.ibeifeng.sparkproject.dao.IAdClickTrendDAO;
import com.ibeifeng.sparkproject.dao.IAdProvinceTop3DAO;
import com.ibeifeng.sparkproject.dao.IAdStatDAO;
import com.ibeifeng.sparkproject.dao.IAdUserClickCountDAO;
import com.ibeifeng.sparkproject.dao.IAreaTop3ProductDAO;
import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.dao.impl.AdBlacklistDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AdClickTrendDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AdProvinceTop3DAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AdStatDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AdUserClickCountDAOImpl;
import com.ibeifeng.sparkproject.dao.impl.AreaTop3ProductDAOImpl;
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
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO(){
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO(){
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

package com.ibeifeng.sparkproject.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;

public class JDBCHelper {
	
	static{
		try {
			String jdbcDrive = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
			Class.forName(jdbcDrive);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static JDBCHelper instance = null;
	
	private  LinkedList<Connection> datasource = new LinkedList<Connection>();
	
	
	
	
	
	private JDBCHelper(){
		int dataSourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
		Boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		for(int i = 0;i < dataSourceSize;i++){
			String url = null;
			String user = null;
			String password = null;
			
			if(isLocal) {
				url = ConfigurationManager.getProperty(Constants.JDBC_URL);
				user = ConfigurationManager.getProperty(Constants.JDBC_USER);
				password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
			} else {
				url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
				user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
				password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
			}
			try {
				Connection con =  DriverManager.getConnection(url, user, password);
				datasource.push(con);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * 设置JDBCHelper单例模式
	 * @return
	 */
	public static JDBCHelper getInstance (){
		if(instance == null){
			synchronized (JDBCHelper.class) {
				if(instance == null)
					instance = new JDBCHelper();
			}
		}
		return instance;
	}
	
	
	/**
	 * 从线程池获取一个线程
	 * @return
	 */
	private synchronized Connection getConnection(){
		while (datasource.isEmpty()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return datasource.poll();
	}
	
	/**
	 * 执行 增加、删除、修改等更新sql
	 * @param sql
	 * @param params
	 * @return
	 */
	public int excuteUpdate(String sql, Object[] params){
		int colmn = 0;
		Connection con = null;
		try {
			con = getConnection();
			con.setAutoCommit(false);
			PreparedStatement pstmt = con.prepareStatement(sql);
			if(params != null && params.length > 0){
				for(int i= 0 ;i< params.length ; i++){
					pstmt.setObject(i+1, params[i]);
				}
			}
			
			colmn  = pstmt.executeUpdate();
			con.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(con != null){
				datasource.push(con);
			}
		}
		return colmn;
	}
	
	/**
	 * 执行查询sql
	 * @param sql
	 * @param params
	 * @param callBack
	 */
	public void excuteQuery(String sql, Object[] params,QueryCallback callBack){
		Connection con = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			con = getConnection();
			con.setAutoCommit(false);
			pstmt = con.prepareStatement(sql);
			
			if(params != null && params.length > 0){
				for(int i= 0 ;i< params.length ; i++){
					pstmt.setObject(i+1, params[i]);
				}
			}
			
			rs = pstmt.executeQuery();
			
			callBack.process(rs);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(con != null){
				datasource.push(con);
			}
		}
		
	}
	
	
	/**
	 * 执行 增加、删除、修改等更新sql
	 * @param sql
	 * @param params
	 * @return
	 */
	public int excuteBatch(String sql, List<Object[]>  paramList){
		int colmn = 0;
		Connection con = null;
		try {
			con = getConnection();
			con.setAutoCommit(false);
			PreparedStatement pstmt = con.prepareStatement(sql);
			for (Object[] params : paramList) {
				if(params != null && params.length > 0){
					for(int i= 0 ;i< params.length ; i++){
						pstmt.setObject(i+1, params[i]);
					}
					pstmt.addBatch();
				}
			}
			colmn  = pstmt.executeUpdate();
			con.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(con != null){
				datasource.push(con);
			}
		}
		return colmn;
	}
	
	/**
	 * 静态内部类：查询回调接口
	 * @author Administrator
	 *
	 */
	public static interface QueryCallback {
		
		/**
		 * 处理查询结果
		 * @param rs 
		 * @throws Exception
		 */
		void process(ResultSet rs) throws Exception;
		
	}
	
}

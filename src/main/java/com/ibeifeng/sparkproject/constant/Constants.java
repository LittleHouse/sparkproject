package com.ibeifeng.sparkproject.constant;

/**
 * 常量接口
 * @author Administrator
 *
 */
public interface Constants {

	/**
	 * 数据库相关的常量
	 */
	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	String SPARK_LOCAL = "spark.local";
	String JDBC_URL_PROD = "jdbc.url.prod";
	String JDBC_USER_PROD = "jdbc.user.prod";
	String JDBC_PASSWORD_PROD = "jdbc.password.prod";
	
	
	/**
	 * 任务相关的常量
	 */
	String PARAM_START_DATE = "startDate";
	String PARAM_END_DATE = "endDate";
	String PARAM_START_AGE = "startAge";
	String PARAM_END_AGE = "endAge";
	String PARAM_PROFESSIONALS = "professionals";
	String PARAM_CITIES = "cities";
	String PARAM_SEX = "sex";
	String PARAM_KEYWORDS = "keywords";
	String PARAM_CATEGORY_IDS = "categoryIds";
	String PARAM_TARGET_PAGE_FLOW = "targetPageFlow";
	
	String SPARK_LOCAL_TASK_SESSION = "spark.local.taskid.session";
	String SPARK_LOCAL_TASK_PAGE = "spark.local.taskid.page";
	
	
	String FIELD_SESSION_ID = "session_id";
	String FIELD_SEARCH_KEYWORDS = "search_keyword";
	String FIELD_CLICK_CATEGORY_IDS = "category_ids";
	String FIELD_AGE = "age";
	String FIELD_PROFESSIONAL = "professional";
	String FIELD_CITY = "city";
	String FIELD_SEX = "sex";
	String FIELD_VISIT_LENGTH = "visit_time";
	String FIELD_STEP_LENGTH = "step_length";
	String FIELD_CATEGORY_ID = "category_id";
	String FIELD_CLICK_COUNT = "click_count";
	String FIELD_ORDER_COUNT = "order_count";
	String FIELD_PAY_COUNT = "pay_count";
	
	
	String SESSION_COUNT = "session_count";
	String TIME_PERIOD_1s_3s = "time_1s_3s";
	String TIME_PERIOD_4s_6s = "time_4s_6s";
	String TIME_PERIOD_7s_9s = "time_7s_9s";
	String TIME_PERIOD_10s_30s = "time_10s_30s";
	String TIME_PERIOD_30s_60s = "time_30s_60s";
	String TIME_PERIOD_1m_3m = "time_1m_3m";
	String TIME_PERIOD_3m_10m = "time_3m_10m";
	String TIME_PERIOD_10m_30m = "time_10m_30m";
	String TIME_PERIOD_30m = "time_30m+";
	String STEP_PERIOD_1_3 = "step_1_3";
	String STEP_PERIOD_4_6 = "step_4_6";
	String STEP_PERIOD_7_9 = "step_7_9";
	String STEP_PERIOD_10_30  = "step_10_30";
	String STEP_PERIOD_30_60  = "step_30_60";
	String STEP_PERIOD_60  = "step_60+";
	
	

	
	
}

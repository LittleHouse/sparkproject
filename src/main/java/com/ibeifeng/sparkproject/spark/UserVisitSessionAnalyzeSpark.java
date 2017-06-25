package com.ibeifeng.sparkproject.spark;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.SessionAggrStat;
import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.test.MockData;
import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.NumberUtils;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.StringUtils;
import com.ibeifeng.sparkproject.util.ValidUtils;

import scala.Tuple2;

public class UserVisitSessionAnalyzeSpark {
	
	private static Logger logger = LoggerFactory.getLogger(UserVisitSessionAnalyzeSpark.class);
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("UserVisitSessionAnalyzeSpark");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = getSQLContext(sc.sc());
		mock(sc,sqlContext);
		
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASK_SESSION);
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskid);
		
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext,taskParam);
		
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sqlContext, actionRDD);
		
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
				sessionid2AggrInfoRDD,taskParam,sessionAggrStatAccumulator);
		
		
		// 万万切记，在持久化Accumulator之前一定要有一个action操作，不然累加变量会是空的
		logger.info("筛选后的session数量：{}",filteredSessionid2AggrInfoRDD.count());
		
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),taskid);
		
		List<Row> sampleAction = actionRDD.takeSample(false, 10);
		
		logger.info(sampleAction.get(0).toString());
		
		pesistSampleSession(sampleAction,taskid);
		
		sc.close();
		
	}


	/**
	 * 根据date 获取 user_visit_action列表
	 * @param sqlContext
	 * @param taskParam
	 * @return
	 */
	private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		
		String sql = "select *  from  user_visit_action where " +
				"date >= '" + startDate + "'" +
				"and date <= '" + endDate + "'";
		DataFrame visitFrame = sqlContext.sql(sql);
		return visitFrame.javaRDD();
				
	}
	
	/**
	 * 对user_visit_action 数据进行session粒度聚合，并且join上用户信息
	 * @param sqlContext
	 * @param actionRDD
	 * @return
	 */
	private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {
		
		// 返回<sessionid,Row>格式的数据
		JavaPairRDD<String, Row> session2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
			
			private static final long serialVersionUID = 1L;

			public scala.Tuple2<String,Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(2), row);
			};
		} );
		
		JavaPairRDD<String, Iterable<Row>> session2ActionsRDD = session2ActionRDD.groupByKey();
		
		//返回<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)> 格式的数据
		JavaPairRDD<Long, String> user2sessionRDD = session2ActionsRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
			
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				String sessionid = tuple._1;
				
				Iterator<Row> iterator = tuple._2.iterator();
				
				StringBuffer searchKeywordsBuffer = new StringBuffer("");
				StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
				
				Long userid = null;
				
				// session的起始和结束时间
				Date startTime = null;
				Date endTime = null;
				// session的访问步长
				int stepLength = 0;
				
				while(iterator.hasNext()){
					Row row = iterator.next();
					if(userid == null){
						userid = row.getLong(1);
					}
					
					String searchKeyword = row.getString(5);
					Long clickCategoryId = row.getLong(6);
					
					if(StringUtils.isNotEmpty(searchKeyword)) {
						if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
							searchKeywordsBuffer.append(searchKeyword + ",");  
						}
					}
					if(clickCategoryId != null) {
						if(!clickCategoryIdsBuffer.toString().contains(
								String.valueOf(clickCategoryId))) {   
							clickCategoryIdsBuffer.append(clickCategoryId + ",");  
						}
					}
					
					// 计算session开始和结束时间
					Date actionTime = DateUtils.parseTime(row.getString(4));
					
					if(startTime == null) {
						startTime = actionTime;
					}
					if(endTime == null) {
						endTime = actionTime;
					}
					
					if(actionTime.before(startTime)) {
						startTime = actionTime;
					}
					if(actionTime.after(endTime)) {
						endTime = actionTime;
					}
					
					// 计算session访问步长
					stepLength++;
				}
				
				String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
				String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
				
				// 计算session访问时长（秒）
				long visitLength = (endTime.getTime() - startTime.getTime()) / 1000; 
				
				
				String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
						+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
						+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
						+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
						+ Constants.FIELD_STEP_LENGTH + "=" + stepLength;

				
				return new Tuple2<Long, String>(userid, partAggrInfo);
			}
		});
		
		String sql = "select * from user_info";
		JavaRDD<Row> useRowRDD = sqlContext.sql(sql).javaRDD();
		JavaPairRDD<Long, Row> useid2userRDD = useRowRDD.mapToPair(new PairFunction<Row, Long, Row>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Row> call(Row row) throws Exception {
				return new Tuple2<Long, Row>(row.getLong(0), row);
			}
		});
		
		JavaPairRDD<Long, Tuple2<String, Row>> usesActionRdd = user2sessionRDD.join(useid2userRDD);
		
		// 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
		JavaPairRDD<String, String> sessionid2fullAggrInfo = usesActionRdd.mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
				String partAggrInfo = tuple._2._1;
				Row userInfoRow = tuple._2._2;
				
				String sessionid = StringUtils.getFieldFromConcatString(
						partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
				
				int age = userInfoRow.getInt(3);
				String professional = userInfoRow.getString(4);
				String city = userInfoRow.getString(5);
				String sex = userInfoRow.getString(6);
				
				String fullAggrInfo = partAggrInfo + "|"
						+ Constants.FIELD_AGE + "=" + age + "|"
						+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
						+ Constants.FIELD_CITY + "=" + city + "|"
						+ Constants.FIELD_SEX + "=" + sex;
				return new Tuple2<String, String>(sessionid, fullAggrInfo);
			}
		});
		return sessionid2fullAggrInfo;
	}

	

	/**
	 * 按照用户信息对session进行筛选，比如： 10< age < 50
	 * @param sessionid2AggrInfoRDD
	 * @param taskParam
	 * @return
	 */
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam,
			final Accumulator<String> sessionAggrStatAccumulator) {  
		
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
		
		if(_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		
		final String parameter = _parameter;
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				String aggrInfo = tuple._2;
				
				// 接着，依次按照筛选条件进行过滤
				// 按照年龄范围进行过滤（startAge、endAge）
				if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, 
						parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
					return false;
				}
				
				// 按照职业范围进行过滤（professionals）
				// 互联网,IT,软件
				// 互联网
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, 
						parameter, Constants.PARAM_PROFESSIONALS)) {
					return false;
				}
				
				// 按照城市范围进行过滤（cities）
				// 北京,上海,广州,深圳
				// 成都
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, 
						parameter, Constants.PARAM_CITIES)) {
					return false;
				}
				
				// 按照性别进行过滤
				// 男/女
				// 男，女
				if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, 
						parameter, Constants.PARAM_SEX)) {
					return false;
				}
				
				// 按照搜索词进行过滤
				// 我们的session可能搜索了 火锅,蛋糕,烧烤
				// 我们的筛选条件可能是 火锅,串串香,iphone手机
				// 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
				// 任何一个搜索词相当，即通过
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, 
						parameter, Constants.PARAM_KEYWORDS)) {
					return false;
				}
				
				// 按照点击品类id进行过滤
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, 
						parameter, Constants.PARAM_CATEGORY_IDS)) {
					return false;
				}
				
				// 主要走到这一步，那么就是需要计数的session
				sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);  
				
				// 计算出session的访问时长和访问步长的范围，并进行相应的累加
				long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
						aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH)); 
				long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
						aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));  
				calculateVisitLength(visitLength); 
				calculateStepLength(stepLength);  
				return true;
			}
			
			/**
			 * 计算访问时长范围
			 * @param visitLength
			 */
			private void calculateVisitLength(long visitLength) {
				if(visitLength >=1 && visitLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);  
				} else if(visitLength >=4 && visitLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);  
				} else if(visitLength >=7 && visitLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);  
				} else if(visitLength >=10 && visitLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);  
				} else if(visitLength > 30 && visitLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);  
				} else if(visitLength > 60 && visitLength <= 180) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);  
				} else if(visitLength > 180 && visitLength <= 600) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);  
				} else if(visitLength > 600 && visitLength <= 1800) {  
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);  
				} else if(visitLength > 1800) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);  
				} 
			}
			
			/**
			 * 计算访问步长范围
			 * @param stepLength
			 */
			private void calculateStepLength(long stepLength) {
				if(stepLength >= 1 && stepLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);  
				} else if(stepLength >= 4 && stepLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);  
				} else if(stepLength >= 7 && stepLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);  
				} else if(stepLength >= 10 && stepLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);  
				} else if(stepLength > 30 && stepLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);  
				} else if(stepLength > 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);    
				}
			}
		});
		
		return filteredSessionid2AggrInfoRDD;
	}
	
	private static void calculateAndPersistAggrStat(String value, long taskid) {
		
		// 从Accumulator统计串中获取值
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.SESSION_COUNT));  
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1s_3s));  
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		// 计算各个访问时长和访问步长的范围
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visit_length_1s_3s / (double)session_count, 2);  
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double)visit_length_4s_6s / (double)session_count, 2);  
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visit_length_7s_9s / (double)session_count, 2);  
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visit_length_10s_30s / (double)session_count, 2);  
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visit_length_30s_60s / (double)session_count, 2);  
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visit_length_1m_3m / (double)session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visit_length_3m_10m / (double)session_count, 2);  
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_10m_30m / (double)session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_30m / (double)session_count, 2);  
		
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double)step_length_1_3 / (double)session_count, 2);  
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double)step_length_4_6 / (double)session_count, 2);  
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double)step_length_7_9 / (double)session_count, 2);  
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double)step_length_10_30 / (double)session_count, 2);  
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double)step_length_30_60 / (double)session_count, 2);  
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double)step_length_60 / (double)session_count, 2);
		
		// 将统计结果封装为Domain对象
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);  
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);  
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);  
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);  
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);  
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);  
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio); 
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);  
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio); 
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);  
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);  
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);  
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);  
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);  
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);  
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);  
		
		// 调用对应的DAO插入统计结果
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);  
	}
	
	

	/**
	 * 把抽取出的100个 session保存到mysql
	 * @param sampleSession2Aggr
	 * @param taskid
	 */
	private static void pesistSampleSession(List<Row> sampleAction, long taskid) {
		List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();
		
		for (Row row : sampleAction) {
			
			SessionDetail sessionDetail = new SessionDetail();
			sessionDetail.setTaskid(taskid);  
			sessionDetail.setUserid(row.getLong(1));  
			sessionDetail.setSessionid(row.getString(2));  
			sessionDetail.setPageid(row.getLong(3));  
			sessionDetail.setActionTime(row.getString(4));
			sessionDetail.setSearchKeyword(row.getString(5));  
			sessionDetail.setClickCategoryId(row.getLong(6));  
			sessionDetail.setClickProductId(row.getLong(7));   
			sessionDetail.setOrderCategoryIds(row.getString(8));  
			sessionDetail.setOrderProductIds(row.getString(9));  
			sessionDetail.setPayCategoryIds(row.getString(10)); 
			sessionDetail.setPayProductIds(row.getString(11));  
			
			sessionDetails.add(sessionDetail);
			
		}
		
		ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
		sessionDetailDAO.insertBatch(sessionDetails);
		
	}

	private static SQLContext getSQLContext(SparkContext sc) {
		Boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(isLocal){
			return new SQLContext(sc);
		}
		else{
			return new HiveContext(sc);
		}
	}
	
	private static void mock(JavaSparkContext sc,SQLContext sqlContext){
		Boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(isLocal){
			MockData.mock(sc, sqlContext);
		}
	}

}

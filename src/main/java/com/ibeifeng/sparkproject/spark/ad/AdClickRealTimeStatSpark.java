package com.ibeifeng.sparkproject.spark.ad;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.IAdBlacklistDAO;
import com.ibeifeng.sparkproject.dao.IAdProvinceTop3DAO;
import com.ibeifeng.sparkproject.dao.IAdStatDAO;
import com.ibeifeng.sparkproject.dao.IAdUserClickCountDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.AdBlacklist;
import com.ibeifeng.sparkproject.domain.AdProvinceTop3;
import com.ibeifeng.sparkproject.domain.AdStat;
import com.ibeifeng.sparkproject.domain.AdUserClickCount;
import com.ibeifeng.sparkproject.util.DateUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;


public class AdClickRealTimeStatSpark {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStatSpark");
		
		//创建stream对象，设置5s更新一次
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		
		//创建kafka DStream，并读取<timestamp province city userid adid>格式的数据
		JavaPairInputDStream<String, String> adClicklogDStream = creatDstream(jssc);
		
		//对kafka读入的DStream，进行黑名单过滤
		JavaPairDStream<String, String> filterAdClicklogDStream = filterByBlacklist(adClicklogDStream);
		
		//按照日期、用户、广告的维度实时统计,返回数据格式 <yyyyMMdd_userid_adid, count>
		JavaPairDStream<String, Long> adUserClickCountDStream = countUseradClick(filterAdClicklogDStream);
		
		//实时更新，数据库中<yyyyMMdd_userid_adid, count>
		saveUseradClickCount(adUserClickCountDStream);
		
		//过滤出ad点击量超过100的黑名单用户
		JavaDStream<Long> blackList = filtBlacklist(adUserClickCountDStream);
		
		//实时更新数据库黑名单
		saveBlackList(blackList);
		
		//按照日期、省份、城市、广告的维度实时统计,返回格式<yyyyMMdd_province_ciyt_adid, count>
		JavaPairDStream<String, Long> adCityClickCountDStream = countCityAdClick(filterAdClicklogDStream);
		
		//实时更新，数据库中<yyyyMMdd_province_ciyt_adid, count>
		saveCityAdClickCount(adCityClickCountDStream);
		
		//业务功能二：实时统计每天每个省份top3热门广告
		JavaDStream<Row> provinceTop3AdClick =calculateProvinceTop3Ad(adCityClickCountDStream); 
		
		//实时更新各省top3广告，到数据库
		saveProvinceTop3AdClick(provinceTop3AdClick);
		
		//构建完spark Stream后，千万记得写 steam的启动、等待执行结束、关闭
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
		
	}


	/**
	 * 创建kafka DStream，并读取数据
	 * @param jssc
	 * @return
	 */
	private static JavaPairInputDStream<String, String> creatDstream( JavaStreamingContext jssc){
		
		//构建kafka参数，主要放置kafka集群的地址
				Map<String,String> kafkaParams = new HashMap<String,String>();
				kafkaParams.put("metadata.broker.list", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
				
				Set<String> topics = new HashSet<String>();
				String kafkatopic = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
				String[]  topicArr = kafkatopic.split(",");
				for (String topic : topicArr) {
					topics.add(topic);
				}
				
				//构建针对kafka集群指定topic的DStream
				JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
						jssc, 
						String.class, 
						String.class, 
						StringDecoder.class, 
						StringDecoder.class, 
						kafkaParams, 
						topics);
				
				
				
				return adRealTimeLogDStream;
	}
	
	
	/**
	 * 对kafka读入的DStream，进行黑名单过滤
	 * @param adRealTimeLogDStream
	 * @return
	 */
	private static JavaPairDStream<String, String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		
		JavaPairDStream<String,String> filteredRdd = adRealTimeLogDStream.transformToPair(new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {

			private static final long serialVersionUID = 1L;

			@SuppressWarnings("resource")
			@Override
			public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
				
				/*********************创建黑名单的rdd*************************************/
				IAdBlacklistDAO adBlackList = DAOFactory.getAdBlacklistDAO();
				List<AdBlacklist> blackList = adBlackList.findAll();
				
				List<Tuple2<Long, Boolean>> blackTuples = new ArrayList<Tuple2<Long, Boolean>>();
				for(AdBlacklist adBlack : blackList){
					long userid = adBlack.getUserid();
					blackTuples.add(new Tuple2<Long, Boolean>(userid, true));
				}
				
				JavaSparkContext sc = new JavaSparkContext(rdd.context());
				JavaPairRDD<Long, Boolean> blackListRdd = sc.parallelizePairs(blackTuples);
				
				
				/********************* 将原始数据rdd映射成<userid, tuple2<string, string>> *************/
				JavaPairRDD<Long, Tuple2<String, String>> adClickuserRdd= rdd.mapToPair(
						new PairFunction<Tuple2<String,String>, Long, Tuple2<String,String>>() {

							private static final long serialVersionUID = 1L;
		
							@Override
							public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
								String[] logParams = tuple._2.split(" ");
								long userid = Long.valueOf(logParams[3]);
								return new Tuple2<Long, Tuple2<String,String>>(userid, tuple);
							}
						});
				
				/**********************将<userid, tuple2<string, string>> 和黑名单RDD join******************/ 
				JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinRdd = adClickuserRdd.leftOuterJoin(blackListRdd);
				
				JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filterRdd = joinRdd.filter(new 
						Function<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, Boolean>() {
					
							private static final long serialVersionUID = 1L;

							@Override
							public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
								Optional<Boolean> isBlack = tuple._2._2;
								if(isBlack.isPresent() && isBlack.get())
									return false;
								else
									return true;
							}
						});
				
				
				JavaPairRDD<String,String> filteredAdClickRDD = filterRdd.mapToPair(
						new PairFunction<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, String,String>() {

							private static final long serialVersionUID = 1L;
		
							@Override
							public Tuple2<String, String> call(
									Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
								return tuple._2._1;
							}
						});
				
				return filteredAdClickRDD;
			}
		});
		
		
		return filteredRdd;
	}
	
	
	/**
	 * 统计ad 点击量并写入数据库
	 * @param adClicklogDStream
	 */
	private static JavaPairDStream<String, Long> countUseradClick(JavaPairDStream<String, String> filterAdClicklogDStream ){
		
			// 一条一条的实时日志
			// timestamp province city userid adid
			// 原始数据格式 <timestamp province city userid adid>
			// 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
			JavaPairDStream<String, Long> adClicklogDStream = filterAdClicklogDStream.mapToPair(
				new PairFunction<Tuple2<String,String>, String, Long>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<String, Long> call(Tuple2<String, String> tuple)
							throws Exception {
						String clickLog = tuple._2;
						String[] clickParams = clickLog.split(" ");
						
						Date logDate = new Date(Long.valueOf(clickParams[0]));
						String logDateStr = DateUtils.formatDateKey(logDate);
						String userid = clickParams[3];
						String adid = clickParams[4];
						
						String tupleKey = logDateStr + "_" + userid + "_" + adid;
						
						return new Tuple2<String, Long>(tupleKey, 1L);
					}
					
				});
		
				//转换出<yyyyMMdd_userid_adid, count>格式数据
				JavaPairDStream<String, Long> adClickCountDStream  = adClicklogDStream.updateStateByKey(
						new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
							
							private static final long serialVersionUID = 1L;

							@Override
							public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
								long count = 0L;
								if(optional.isPresent())
									count = optional.get();
								for(Long value: values){
									count += value;
								}
								return Optional.of(count);
							}
				});
				
			return adClickCountDStream;
		
	}
	
	
	private static void saveUseradClickCount (JavaPairDStream<String, Long> adClickCountDStream){
		//把<yyyyMMdd_userid_adid, count>写入数据库
		adClickCountDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
						
						List<AdUserClickCount> adUserCountList = new ArrayList<AdUserClickCount>();
						while(iterator.hasNext()){
							Tuple2<String, Long> tuple = iterator.next();
							long count = tuple._2;
							String[] adCounts = tuple._1.split("_");
							
							String data = adCounts[0];
							String logDate = DateUtils.formatDate(DateUtils.parseDateKey(data));
							
							long userid = Long.valueOf(adCounts[1]);
							long adid = Long.valueOf(adCounts[2]);
							
							AdUserClickCount ad = new AdUserClickCount();
							ad.setAdid(adid);
							ad.setClickCount(count);
							ad.setDate(logDate);
							ad.setUserid(userid);
							adUserCountList.add(ad);
						}
						
						IAdUserClickCountDAO  adCountDAO = DAOFactory.getAdUserClickCountDAO();
						adCountDAO.updateBatch(adUserCountList);
					}
				});

			return null;
			};
		});
	}
	
	/**
	 * 过滤出ad点击量超过100的黑名单用户，
	 * @param adUserClickCountDStream，格式<yyyyMMdd_userid_adid, count>
	 */
	private static JavaDStream<Long> filtBlacklist(JavaPairDStream<String, Long> adUserClickCountDStream) {
		
		//过滤出date, userid, adid 数量>100 的记录
		JavaPairDStream<String, Long> blackListDStream = adUserClickCountDStream.filter(new Function<Tuple2<String,Long>, Boolean>() {
			
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, Long> tuple) throws Exception {
				String[] adClickMsgs = tuple._1.split("_");
				
				String date = DateUtils.formatDate(DateUtils.parseDateKey(adClickMsgs[0]));
				long userid = Long.valueOf(adClickMsgs[1]);
				long adid = Long.valueOf(adClickMsgs[2]);
				
				IAdUserClickCountDAO adUserDAO = DAOFactory.getAdUserClickCountDAO();
				int count =  adUserDAO.findClickCountByMultiKey(date, userid, adid);
				if(count > 100){
					return true ;
				}
				else{
					return false;
				}
			}
		});
		
		
		// 把<yyyyMMdd_userid_adid, count>转为<userid>,方便去重
		JavaDStream<Long> blockListRdd =  blackListDStream.map(new Function<Tuple2<String,Long>, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Tuple2<String, Long> tuple) throws Exception {
				String log = tuple._1;
				long userid = Long.valueOf(log.split("_")[1]);
				return userid;
			}
		});
		
		//对<userid>格式数据去重
		JavaDStream<Long> blockListDisRdd = blockListRdd.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
				return rdd.distinct();
			}
		});
		
		return blockListDisRdd;
		
	}
	
	/**
	 * 实时更新数据库黑名单记录
	 * @param blockListDisRdd
	 */
	private static void saveBlackList (JavaDStream<Long> blockListDisRdd){

		//对DStream的每个Rdd的每个partition进行批量插入数据库
		blockListDisRdd.foreachRDD(new Function<JavaRDD<Long>, Void>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<Long> rdd) throws Exception {
				
				rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
					
					
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Long> iterator) throws Exception {
						
						List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();
						while(iterator.hasNext()){
							long blackUserid = iterator.next();
							AdBlacklist adBlacklist = new AdBlacklist();
							adBlacklist.setUserid(blackUserid); 
							
							adBlacklists.add(adBlacklist);
						}
						IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
						adBlacklistDAO.insertBatch(adBlacklists); 
						
					}
				});

				return null;
			}
		});
		
	}
	
	
	


	/**
	 * 按照日期、省份、城市、广告的维度，进行流量实时统计,返回格式<yyyyMMdd_province_ciyt_adid, count>
	 * 原始数据格式  <timestamp province city userid adid>
	 * @param filterAdClicklogDStream
	 * @return
	 */
	private static JavaPairDStream<String, Long> countCityAdClick(JavaPairDStream<String, String> filterAdClicklogDStream) {
		
		JavaPairDStream<String, Long> pairDstream = filterAdClicklogDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
				String[] clickParams = tuple._2.split(" ");
				String date = DateUtils.formatDateKey(new Date(Long.valueOf(clickParams[0])));
				String province = clickParams[1];
				String city = clickParams[2];
				long adid = Long.valueOf(clickParams[4]);
				
				String keyStr = date + "_" + province + "_" + city + "_" + adid;
				return new Tuple2<String, Long>(keyStr, 1L);
			}
		});
		
		JavaPairDStream<String, Long> aggregatedDStream = pairDstream.updateStateByKey(
				new Function2<List<Long>, Optional<Long>, Optional<Long>>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
						
						long count = 0L;
						
						//optional是上次DStream的计算结果，如果有就取出来
						if(optional.isPresent()){
							count = optional.get();
						}
						//values 是本次DStream 中的value队列，累加到初始值中
						for(Long value: values){
							count += value;
						}
						return Optional.of(count);
					}
				});
		
		return aggregatedDStream;
	}
	
	

	/**
	 * 实时更新，数据库中<yyyyMMdd_province_ciyt_adid, count>
	 * @param adCityClickCountDStream
	 */
	private static void saveCityAdClickCount(JavaPairDStream<String, Long> adCityClickCountDStream) {
		adCityClickCountDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<String, Long> Rdd) throws Exception {
				Rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
						List<AdStat> adStats = new ArrayList<AdStat>();
						
						while(iterator.hasNext()) {
							Tuple2<String, Long> tuple = iterator.next();
							
							String[] keySplited = tuple._1.split("_");
							String date = DateUtils.formatDate(new Date(Long.valueOf(keySplited[0])));
							String province = keySplited[1];
							String city = keySplited[2];
							long adid = Long.valueOf(keySplited[3]);  
							
							long clickCount = tuple._2;
							
							AdStat adStat = new AdStat();
							adStat.setDate(date); 
							adStat.setProvince(province);  
							adStat.setCity(city);  
							adStat.setAdid(adid); 
							adStat.setClickCount(clickCount);  
							
							adStats.add(adStat);
						}
						
						IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
						adStatDAO.updateBatch(adStats);
					}
				});
				return null;
			}
		});
	}
	
	

	/**
	 * 实时统计每天每个省份top3热门广告,返回格式 <date,province,ad_id,click_count>
	 * @param adCityClickCountDStream 格式 <yyyyMMdd_province_ciyt_adid, count>
	 * @param sc 
	 * @return 
	 */
	private static JavaDStream<Row> calculateProvinceTop3Ad(JavaPairDStream<String, Long> adCityClickCountDStream) {
		
		//返回格式<yyyyMMdd_province_adid, count>
		JavaPairDStream<String, Long> adProvinceClickCountDStream = adCityClickCountDStream.mapToPair(
				new PairFunction<Tuple2<String,Long>, String, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
						long count = tuple._2;
						String[] logParams = tuple._1.split("_");
						String date = logParams[0];
						String province = logParams[1];
						String adid = logParams[3];
						
						String key = date + "_" + province + "_" + adid;
						return new Tuple2<String, Long>(key, count);
					}
				});
		
		
		JavaPairDStream<String, Long> adProvinceClickAggrDStream =adProvinceClickCountDStream.reduceByKey(new Function2<Long, Long, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		});
		
		
		JavaDStream<Row>  rowsRDD = adProvinceClickAggrDStream.map(new Function<Tuple2<String,Long>, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Long> tuple) throws Exception {
				long clickCount = tuple._2;
				String[] logParams = tuple._1.split("_");
				String date =  logParams[0];
				String province = logParams[1];
				long adid = Long.valueOf( logParams[2]);
				
				return RowFactory.create(date, province, adid, clickCount);
			}
		});
		
		
		JavaDStream<Row> provinceTop3AdClick = rowsRDD.transform(new Function<JavaRDD<Row>, JavaRDD<Row>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<Row> call(JavaRDD<Row> rdd) throws Exception {
				
				HiveContext sqlContext = new HiveContext(rdd.context());
				
				StructType schema = DataTypes.createStructType(Arrays.asList(
						DataTypes.createStructField("date", DataTypes.StringType, true),
						DataTypes.createStructField("province", DataTypes.StringType, true),
						DataTypes.createStructField("ad_id", DataTypes.LongType, true),
						DataTypes.createStructField("click_count", DataTypes.LongType, true)));
				
				DataFrame dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rdd, schema);
				
				dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");
				
				// 使用Spark SQL执行SQL语句，配合开窗函数，统计出各身份top3热门的广告
				DataFrame provinceTop3AdDF = sqlContext.sql(
						"SELECT "
							+ "date,"
							+ "province,"
							+ "ad_id,"
							+ "click_count "
						+ "FROM ( "
							+ "SELECT "
								+ "date,"
								+ "province,"
								+ "ad_id,"
								+ "click_count,"
								+ "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank "
							+ "FROM tmp_daily_ad_click_count_by_prov "
						+ ") t "
						+ "WHERE rank>=3"
				);  
				
				return provinceTop3AdDF.javaRDD();
			}
		});
		
		return provinceTop3AdClick;
	}
	
	
	
	/**
	 * 实时更新各省top3广告，到数据库
	 * @param provinceTop3AdClick
	 */
	private static void saveProvinceTop3AdClick(JavaDStream<Row> provinceTop3AdClick) {
		provinceTop3AdClick.foreachRDD(new Function<JavaRDD<Row>, Void>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<Row> rdd) throws Exception {
				
				rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Row> iterator) throws Exception {
						List<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();
						
						while(iterator.hasNext()) {
							Row row = iterator.next();
							String date = row.getString(0);
							String province = row.getString(1);
							long adid = row.getLong(2);
							long clickCount = row.getLong(3);
							
							AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
							adProvinceTop3.setDate(date); 
							adProvinceTop3.setProvince(province); 
							adProvinceTop3.setAdid(adid);  
							adProvinceTop3.setClickCount(clickCount); 
							
							adProvinceTop3s.add(adProvinceTop3);
						}
						
						IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
						adProvinceTop3DAO.updateBatch(adProvinceTop3s);  
						
					}
				});
				return null;
			}
		});
		
	}

}

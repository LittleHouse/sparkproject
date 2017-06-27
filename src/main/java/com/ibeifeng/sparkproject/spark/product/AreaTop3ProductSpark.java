package com.ibeifeng.sparkproject.spark.product;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.IAreaTop3ProductDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.AreaTop3Product;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;

import scala.Tuple2;

public class AreaTop3ProductSpark {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("AreaTop3ProductSpark");
		SparkUtils.setMaster(conf);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		
		//注册自定义函数
		sqlContext.udf().register("concat_long_string",new ConcatLongStringUDF(),DataTypes.StringType);
		sqlContext.udf().register("group_concat_distinct",new GroupConcatDistinctUDAF());
		
		
		SparkUtils.mockData(sc, sqlContext);
		
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
		Task task = taskDAO.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject( task.getTaskParam());
		String startDate = taskParam.getString(Constants.PARAM_START_DATE);
		String endDate = taskParam.getString(Constants.PARAM_END_DATE);
		
		//查询hive表获取 商品点击记录
		JavaPairRDD<Long, Row> cityid2clickActionRDD =  getClickActionRDDByDate(sqlContext, startDate, endDate);
		
		//查询mysql 的city_info表，并转换为RDD
		JavaPairRDD<Long, Row> cityid2cityInfoRDD  = getCityInfoRDD(sqlContext);
		
		// 商品点击表 join city_info表，并把结果注册为临时表
		generateTempClickProductBasicTable(sqlContext, cityid2clickActionRDD, cityid2cityInfoRDD); 
		
		
		// 生成各区域各商品点击次数的临时表
		generateTempAreaPrdocutClickCountTable(sqlContext);
		
		//生成包含商品完整信息的临时表
		generateTempAreaFullProductClickCountTable(sqlContext);
		
		// 使用开窗函数获取各个区域内点击次数排名前3的热门商品
		JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);
		
		System.out.println(areaTop3ProductRDD.count());
		
		persistAreaTop3Product(taskid, areaTop3ProductRDD.collect());
		
		sc.close();
	}


	/**
	 * 查询符合条件的session记录
	 * @param sqlContext
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private static JavaPairRDD<Long, Row> getClickActionRDDByDate(SQLContext sqlContext, String startDate, String endDate) {
		
		String sql = "select city_id,click_product_id product_id from user_visit_action "
				+ "where click_product_id IS NOT NULL "
				+ "and date >= '" + startDate + "' "
				+ "and date <= '" + endDate + "'";
		
		DataFrame dataFrame = sqlContext.sql(sql);
		JavaRDD<Row> clickLogRdd = dataFrame.javaRDD();
		JavaPairRDD<Long, Row> cityProductRDD = clickLogRdd.mapToPair(new PairFunction<Row, Long, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row row) throws Exception {
				Long cityid = row.getLong(0);
				return new Tuple2<Long, Row>(cityid, row);
			}
		});
		
		return cityProductRDD;
	}
	
	

	/**
	 * 查询mysql中的city_info表，并转为RDD
	 * @param sqlContext
	 * @return
	 */
	private static JavaPairRDD<Long, Row> getCityInfoRDD(SQLContext sqlContext) {
		String url = null;
		String user = null;
		String password = null;
		Boolean local =  ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
		}
		else{
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
		}
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", url);
		options.put("user", user);
		options.put("password", password);
		options.put("dbtable", "city_info");
		
		DataFrame cityFrame = sqlContext.read().format("jdbc").options(options).load();
		JavaRDD<Row> cityRDD = cityFrame.javaRDD();
		JavaPairRDD<Long, Row> cityInfoRDD = cityRDD.mapToPair(new PairFunction<Row, Long, Row>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row row) throws Exception {
				long cityid = row.getLong(0);
				return new Tuple2<Long, Row>(cityid, row);
			}
		});
		return cityInfoRDD;
	}
	
	

	/**
	 * 将 <cityId,cityName,area,productid> 格式数据转换为tmp_clk_prod_basic 的DataFrame
	 * @param sqlContext
	 * @param cityid2clickActionRDD
	 * @param cityid2cityInfoRDD
	 */
	private static void generateTempClickProductBasicTable(SQLContext sqlContext,
			JavaPairRDD<Long, Row> cityid2clickActionRDD, JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
		
		JavaPairRDD<Long, Tuple2<Row, Row>> city2clickRDD = cityid2clickActionRDD.join(cityid2cityInfoRDD);
		JavaRDD<Row> cityProductRDD = city2clickRDD.map(new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
				Row productRow = tuple._2._1;
				Row cityRow = tuple._2._2;
				Long cityId = tuple._1;
				String cityName = cityRow.getString(1);
				String area = cityRow.getString(2);
				Long productid = productRow.getLong(1);
				return RowFactory.create(cityId,cityName,area,productid);
			}
		});
		
		// 基于JavaRDD<Row>的格式，就可以将其转换为DataFrame
		List<StructField> structfields = new ArrayList<StructField>();
		structfields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true) );
		structfields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true) );
		structfields.add(DataTypes.createStructField("area", DataTypes.StringType, true) );
		structfields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true) );
		
		StructType schema = DataTypes.createStructType(structfields);
		DataFrame df = sqlContext.createDataFrame(cityProductRDD, schema);
		
		// 将DataFrame中的数据，注册成临时表（tmp_clk_prod_basic）
		df.registerTempTable("tmp_click_product_basic");  
	}
	
	


	 /**
	  * 生成各区域各商品点击次数临时表
	  * @param sqlContext
	  */
	private static void generateTempAreaPrdocutClickCountTable(SQLContext sqlContext) {
		
		String sql = "select "
				+ "area, "
				+ "product_id, "
				+ "count(*) click_count, "
				+ "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos "  
				+ "FROM tmp_click_product_basic "
				+ "GROUP BY area,product_id ";
		
		DataFrame df = sqlContext.sql(sql);
		
		// 各区域各商品的点击次数（以及额外的城市列表）
		df.registerTempTable("tmp_area_product_click_count");
	}
	
	/**
	 * 生成包含商品完整信息的临时表
	 * @param sqlContext
	 */
	private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
		String sql = "SELECT "
				+ "tapcc.area,"
				+ "tapcc.product_id,"
				+ "tapcc.click_count,"
				+ "tapcc.city_infos,"
				+ "pi.product_name,"
				+ "if(get_json_object(pi.extend_info,'product_status')=0,'自营商品','第三方商品') product_status "
			+ "FROM tmp_area_product_click_count tapcc "
			+ "JOIN product_info pi ON tapcc.product_id=pi.product_id ";
		
		DataFrame df = sqlContext.sql(sql);
		
		df.registerTempTable("tmp_area_fullprod_click_count");   
	}

	/**
	 * 使用开窗函数 分组取topN
	 * @param sqlContext
	 * @return
	 */
	private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {
		
		String sql = 
				"SELECT "
					+ "area,"
					+ "CASE "
						+ "WHEN area='China North' OR area='China East' THEN 'A Level' "
						+ "WHEN area='China South' OR area='China Middle' THEN 'B Level' "
						+ "WHEN area='West North' OR area='West South' THEN 'C Level' "
						+ "ELSE 'D Level' "
					+ "END area_level,"
					+ "product_id,"
					+ "click_count,"
					+ "city_infos,"
					+ "product_name,"
					+ "product_status "
				+ "FROM ("
					+ "SELECT "
						+ "area,"
						+ "product_id,"
						+ "click_count,"
						+ "city_infos,"
						+ "product_name,"
						+ "product_status,"
						+ "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
					+ "FROM tmp_area_fullprod_click_count "
				+ ") t "
				+ "WHERE rank<=3";
		
		DataFrame df = sqlContext.sql(sql);
		
		return df.javaRDD();
	}
	
	/**
	 * 将计算出来的各区域top3热门商品写入MySQL中
	 * @param rows
	 */
	private static void persistAreaTop3Product(long taskid, List<Row> rows) {
		List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();
		
		for(Row row : rows) {
			AreaTop3Product areaTop3Product = new AreaTop3Product();
			areaTop3Product.setTaskid(taskid); 
			areaTop3Product.setArea(row.getString(0));  
			areaTop3Product.setAreaLevel(row.getString(1));  
			areaTop3Product.setProductid(row.getLong(2)); 
			areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));    
			areaTop3Product.setCityInfos(row.getString(4));  
			areaTop3Product.setProductName(row.getString(5));  
			areaTop3Product.setProductStatus(row.getString(6));  
			areaTop3Products.add(areaTop3Product);
		}
		
		IAreaTop3ProductDAO areTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
		areTop3ProductDAO.insertBatch(areaTop3Products);
	}

}

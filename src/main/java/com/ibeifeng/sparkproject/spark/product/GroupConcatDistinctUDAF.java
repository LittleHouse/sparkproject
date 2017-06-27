package com.ibeifeng.sparkproject.spark.product;

import java.util.Arrays;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

	private static final long serialVersionUID = 1L;
	
	//指定输入数据的字段和类型
	private StructType inputStream  = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));
	
	// 指定缓冲数据的字段和类型
	private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));
	
	//指定返回类型
	private DataType dataType = DataTypes.StringType;
	
	//指定是否是确定性的
	private boolean deterministic = true;
	
	

	@Override
	public StructType inputSchema() {
		return inputStream;
	}

	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return dataType;
	}

	@Override
	public boolean deterministic() {
		return deterministic;
	}
	

	//初始化，指定初始值
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}
	
	
    //对每个节点的数据进行一行一行的聚合，结果放入缓冲buffer
	@Override
	public void update(MutableAggregationBuffer buffer, Row row) {
		
		//缓冲中已经拼接过的城市信息
		String bufferCityInfo = buffer.getString(0);
		//新加入的一行中的城市信息
		String cityInfo = row.getString(0);
		
		//把新加入的城市拼接进  缓冲城市信息中，重复的不添加
		if(!bufferCityInfo.contains(cityInfo)){
			if("".equals(bufferCityInfo)){
				bufferCityInfo += cityInfo;
			}
			else{
				bufferCityInfo = bufferCityInfo + "," + cityInfo;
			}
			buffer.update(0, bufferCityInfo);
		}
	}
	
	
	// 对每个节点结算的结果进行聚合，聚合结果放入 buffer
	@Override
	public void merge(MutableAggregationBuffer buffer, Row row) {
		//缓冲中已经拼接过的城市信息
		String bufferCityInfo = buffer.getString(0);
		//某个节点的update函数聚合的结果
		String cityInfos = row.getString(0);
		
		for(String cityInfo: cityInfos.split(",")){
			
			if(!bufferCityInfo.contains(cityInfo)){
				if("".equals(bufferCityInfo)){
					bufferCityInfo += cityInfo;
				}
				else{
					bufferCityInfo = bufferCityInfo + "," + cityInfo;
				}
				buffer.update(0, bufferCityInfo);
			}
		}
	}

	//返回结果值
	@Override
	public Object evaluate(Row row) {
		return row.getString(0);
	}



	


}

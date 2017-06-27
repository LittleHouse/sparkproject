package com.ibeifeng.sparkproject.spark.product;

import org.apache.spark.sql.api.java.UDF3;

public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String call(Long t1, String t2, String split) throws Exception {
		
		return t1 + split + t2;
	}

}

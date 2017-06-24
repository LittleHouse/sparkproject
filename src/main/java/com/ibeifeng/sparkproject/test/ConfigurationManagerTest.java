package com.ibeifeng.sparkproject.test;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;

public class ConfigurationManagerTest {

	public static void main(String[] args) {
		String value = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
		System.out.println(value);

	}

}

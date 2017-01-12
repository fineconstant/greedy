package org.kduda.greedy.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SparkConsts {
	public static String APP_NAME;
	public static String MASTER;

	@Value("${greedy.spark.appName}")
	public static void setAppName(String appName) {
		APP_NAME = appName;
	}

	@Value("${greedy.spark.master}")
	public static void setDatabase(String master) {
		MASTER = master;
	}
}

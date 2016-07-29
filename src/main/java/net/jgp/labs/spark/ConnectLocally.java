package net.jgp.labs.spark;

import org.apache.spark.sql.SparkSession;

public class ConnectLocally {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Hello Spark").master("local").getOrCreate();
		System.out.println("Hello, Spark v." + spark.version());
	}

}

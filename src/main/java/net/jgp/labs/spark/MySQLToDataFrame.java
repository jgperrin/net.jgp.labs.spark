package net.jgp.labs.spark;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class MySQLToDataFrame {

	public static void main(String[] args) {
		MySQLToDataFrame app = new MySQLToDataFrame();
		app.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("DataFrame from MySQL JDBC COnnection").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		java.util.Properties props = new Properties();
		props.put("user", "eateryuser");
		props.put("password", "in4mixR0x");
		props.put("useSSL", "false");
		props.put("serverTimezone", "EST");
		DataFrame df = sqlContext.read().jdbc("jdbc:mysql://mocka:3306/nceatery", "post", props);
		df = df.orderBy(df.col("date_last_update"));
		df.show();
	}
}

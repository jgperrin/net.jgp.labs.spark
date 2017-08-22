package net.jgp.labs.spark.l801_concurrency_in_progress.l000;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Reader {

	public static void main(String[] args) {
		Reader app = new Reader();
		app.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("Concurrency Lab 001").setMaster(Config.MASTER);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		conf = spark.sparkContext().conf();
		System.out.println(conf.get("hello"));

		Dataset<Row> df = spark.sql("SELECT * from myView");
		df.show();
	}
}

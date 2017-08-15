package net.jgp.labs.spark.l800_concurrency.l001;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Loader {

	public static void main(String[] args) {
		Loader app = new Loader();
		app.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("Concurrency Lab 001").setMaster(Config.MASTER).set("hello", "world");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		String filename = "data/tuple-data-file.csv";
		Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true").option("header", "false")
				.load(filename);
		df.show();

		try {
			df.createGlobalTempView("myView");
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			System.out.println("Hmmm... Something interrupted the thread: " + e.getMessage());
		}

	}
}

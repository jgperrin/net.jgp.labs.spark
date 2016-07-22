package net.jgp.labs.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class CsvToDataFrame {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		CsvToDataFrame app = new CsvToDataFrame();
		app.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("DataFrame from Text File").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		String filename = "data/tuple-data-file.csv";
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "false").load(filename);
		df.show();
	}
}

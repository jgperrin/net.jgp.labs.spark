package net.jgp.labs.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class TextFileToDataFrame {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		TextFileToDataFrame app = new TextFileToDataFrame();
		app.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("DataFrame from Text File").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		String filename = "data/simple-data-file.txt";
		DataFrame df = sqlContext.read().text(filename);
		df.show();
	}
}

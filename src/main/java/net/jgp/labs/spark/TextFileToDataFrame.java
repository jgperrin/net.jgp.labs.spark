package net.jgp.labs.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TextFileToDataFrame {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		TextFileToDataFrame app = new TextFileToDataFrame();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder()
				.appName("DataFrame from Text File")
				.master("local[*]")
				.getOrCreate();

		String filename = "data/simple-data-file.txt";
		Dataset<Row> df = spark.read().text(filename);
		df.show();
	}
}

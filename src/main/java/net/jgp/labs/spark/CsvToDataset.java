package net.jgp.labs.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDataset {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		CsvToDataset app = new CsvToDataset();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("CSV to Dataset").master("local").getOrCreate();

		String filename = "data/tuple-data-file.csv";
		Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true")
				.option("header", "false").load(filename);
		df.show();

		int count = df.columns().length;
		for (int i = 0; i < count; i++) {
			String oldColName = "_c" + i;
			String newColName = "C" + i;
			df = df.withColumn(newColName, df.col(oldColName)).drop(oldColName);
		}
		df.show();
	}
}

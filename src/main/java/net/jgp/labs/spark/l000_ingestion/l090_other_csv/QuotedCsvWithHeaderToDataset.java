package net.jgp.labs.spark.l000_ingestion.l090_other_csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class QuotedCsvWithHeaderToDataset {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		QuotedCsvWithHeaderToDataset app = new QuotedCsvWithHeaderToDataset();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("CSV to Dataset").master("local").getOrCreate();

		String filename = "data/csv-quoted.txt";
		Dataset<Row> df = spark.read().option("inferSchema", "true").option("header", "true").csv(filename);
		df.show();
		df.printSchema();

	}
}

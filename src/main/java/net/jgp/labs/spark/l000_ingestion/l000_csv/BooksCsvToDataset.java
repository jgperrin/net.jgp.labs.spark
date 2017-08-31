package net.jgp.labs.spark.l000_ingestion.l000_csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BooksCsvToDataset {

	public static void main(String[] args) {
		BooksCsvToDataset app = new BooksCsvToDataset();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("Book CSV to Dataset").master("local").getOrCreate();

		String filename = "data/books.csv";
		Dataset<Row> df = spark.read().format("csv").option("inferSchema", "false").option("header", "true")
				.load(filename);
		df.show();
		
		// In this case everything is a string
		df.printSchema();
	}
}

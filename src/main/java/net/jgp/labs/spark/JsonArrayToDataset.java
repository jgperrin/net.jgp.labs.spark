package net.jgp.labs.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A basic example of JSON array.
 * @author jgp
 */
public class JsonArrayToDataset {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		JsonArrayToDataset app = new JsonArrayToDataset();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("JSON to Dataset").master("local").getOrCreate();

		String filename = "data/array.json";
		long start = System.currentTimeMillis();
		Dataset<Row> df = spark.read().json(filename);
		long stop = System.currentTimeMillis();
		System.out.println("Processing took " + (stop - start) + " ms");
		df.show();
		df.printSchema();
	}
}

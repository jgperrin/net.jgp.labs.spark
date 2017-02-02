package net.jgp.labs.spark.l000_ingestion;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.IntegerType;

public class ArrayToDataset {

	public static void main(String[] args) {
		ArrayToDataset app = new ArrayToDataset();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("Array to Dataset").master("local").getOrCreate();

		Integer[] l = new Integer[] { 1, 2, 3, 4, 5, 6, 7 };
		List<Integer> data = Arrays.asList(l);
		Dataset<Row> df = spark.createDataFrame(data, IntegerType.class);

		// TODO this is display an empty array for now
		df.show();
	}
}
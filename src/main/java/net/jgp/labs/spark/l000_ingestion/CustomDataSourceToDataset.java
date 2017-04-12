package net.jgp.labs.spark.l000_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Turning a custom data source to a Dataset/Dataframe.
 * 
 * @author jgp
 */
public class CustomDataSourceToDataset {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		CustomDataSourceToDataset app = new CustomDataSourceToDataset();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("Custom data set to Dataset").master("local").getOrCreate();

		String filename = "data/array-complex.json";
		long start = System.currentTimeMillis();
		Dataset<Row> df = spark.read().format("net.jgp.labs.spark.x.datasource.CharCounterDataSource")
				.option("char", "a") // count the number of 'a'
				.load(filename); // local file (line 40 in the stacks below)
		long stop = System.currentTimeMillis();
		System.out.println("Processing took " + (stop - start) + " ms");
		//df.show();
		df.printSchema();
	}
}

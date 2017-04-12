package net.jgp.labs.spark.l000_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.labs.spark.x.datasource.CharCounterDataSource;

/**
 * Turning a custom data source to a Dataset/Dataframe.
 * 
 * @author jgp
 */
public class CustomDataSourceToDataset {
	private static Logger log = LoggerFactory.getLogger(CharCounterDataSource.class);

	public static void main(String[] args) {
		log.info("Working directory: [{}]", System.getProperty("user.dir"));
		log.debug("Working directory: [{}]", System.getProperty("user.dir"));
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
		df.printSchema();
		df.show();
	}
}

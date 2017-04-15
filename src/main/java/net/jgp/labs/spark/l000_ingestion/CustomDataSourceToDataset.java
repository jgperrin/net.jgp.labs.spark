package net.jgp.labs.spark.l000_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.labs.spark.x.utils.K;

/**
 * Turning a custom data source to a Dataset/Dataframe.
 * 
 * @author jgp
 */
public class CustomDataSourceToDataset {
	private static Logger log = LoggerFactory.getLogger(CustomDataSourceToDataset.class);

	public static void main(String[] args) {
		log.debug("Working directory: [{}].", System.getProperty("user.dir"));
		CustomDataSourceToDataset app = new CustomDataSourceToDataset();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("Custom data set to Dataset").master("local").getOrCreate();

		String filename = "data/array-complex.json";
		long start = System.currentTimeMillis();
		Dataset<Row> df = spark.read().format("net.jgp.labs.spark.x.datasource.SubStringCounterDataSource")
				.option(K.COUNT + "0", "a") // count the number of 'a'
				.option(K.COUNT + "1", "b") // count the number of 'b'
				.option(K.COUNT + "2", "color") // count the number of 'color'
				.load(filename); // local file
		long stop = System.currentTimeMillis();
		log.info("Processing took {} ms", stop - start);
		df.printSchema();
		df.show();
	}
}

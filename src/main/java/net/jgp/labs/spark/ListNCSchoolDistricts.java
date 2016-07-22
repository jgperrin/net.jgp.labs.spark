/**
 * First remote submission.
 */
package net.jgp.labs.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import net.jgp.commons.download.DownloadManager;

/**
 * @author jgp
 *
 */
public class FirstJob {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String filename = DownloadManager.getFilename(
				"https://opendurham.nc.gov/explore/dataset/north-carolina-school-performance-data/download/?format=json&timezone=America/New_York");
		System.out.println("File " + filename + " downloaded");

		SparkConf conf = new SparkConf().setAppName("myFirstJob").setMaster("spark://10.0.100.120:7077");
		// SparkConf conf = new
		// SparkConf().setAppName("myFirstJob").setMaster("local[*]");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		javaSparkContext.setLogLevel("WARN");
		SQLContext sqlContext = new SQLContext(javaSparkContext);

		System.out.println("Hello, Remote Spark v." + javaSparkContext.version());

		String fileToAnalyze = "/Pool/" + filename;
		System.out.println("File to analyze: " + fileToAnalyze);

		DataFrame df;
		df = sqlContext.read().option("dateFormat", "yyyy-mm-dd").json(fileToAnalyze);
		// .json("./src/main/resources/north-carolina-school-performance-data.json");
		df = df.withColumn("district", df.col("fields.district"));
		df = df.groupBy("district").count().orderBy(df.col("district"));
		df.show(150);
	}
}

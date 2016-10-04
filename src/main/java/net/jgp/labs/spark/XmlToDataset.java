package net.jgp.labs.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class XmlToDataset {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		XmlToDataset app = new XmlToDataset();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("XML to Dataset").master("local").getOrCreate();

		String filename = "data/budget-2017.xml";
		Dataset<Row> df = spark.read().format("com.databricks.spark.xml").load(filename);
		df.show();
	}
}

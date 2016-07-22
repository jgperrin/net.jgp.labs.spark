package net.jgp.labs.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SimplePredictionFromTextFile {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		SimplePredictionFromTextFile app = new SimplePredictionFromTextFile();
		app.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("Simple Prediction from Text File").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		String filename = "data/tuple-data-file.csv";
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "false").load(filename);
		df = df.withColumn("label", df.col("C0")).drop("C0");
		df = df.withColumn("features", df.col("C1")).drop("C1");
		df.show();
		LogisticRegression lr = new LogisticRegression().setMaxIter(10);

		// Fit the model to the data.
		LogisticRegressionModel model = lr.fit(df);

		// Inspect the model: get the feature weights.
		// Vector weights = model.weights();

		// Given a dataset, predict each point's label, and show the results.
		model.transform(df).show();
	}
}

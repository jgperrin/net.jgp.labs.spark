package net.jgp.labs.spark;

import static org.apache.spark.sql.functions.callUDF;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.jgp.labs.spark.udf.VectorBuilder;

public class SimplePredictionFromTextFile implements Serializable {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		SimplePredictionFromTextFile app = new SimplePredictionFromTextFile();
		app.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("Simple prediction from Text File").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		sqlContext.udf().register("vectorBuilder", new VectorBuilder(), new VectorUDT());

		String filename = "data/tuple-data-file.csv";
		StructType schema = new StructType(
				new StructField[] { new StructField("C0", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("C1", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("features", new VectorUDT(), true, Metadata.empty()), });

		DataFrame df = sqlContext.read().format("com.databricks.spark.csv").schema(schema).option("header", "false")
				.load(filename);
		df = df.withColumn("valuefeatures", df.col("C0")).drop("C0");
		df = df.withColumn("label", df.col("C1")).drop("C1");
		df.printSchema();
		// df.show();

		df = df.withColumn("features", callUDF("vectorBuilder", df.col("valuefeatures")));
		df.printSchema();
		df.show();

		LinearRegression lr = new LinearRegression().setMaxIter(20);//.setRegParam(1).setElasticNetParam(1);
		

		// Fit the model to the data.
		LinearRegressionModel model = lr.fit(df);

		// Inspect the model: get the feature weights.
		Vector weights = model.weights();

		// Given a dataset, predict each point's label, and show the results.
		model.transform(df).show();

		LinearRegressionTrainingSummary trainingSummary = model.summary();
		System.out.println("numIterations: " + trainingSummary.totalIterations());
		System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
		trainingSummary.residuals().show();
		System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
		System.out.println("r2: " + trainingSummary.r2());

		double intersect = model.intercept();
		System.out.println("Interesection: " + intersect);
		double regParam = model.getRegParam();
		System.out.println("Regression parameter: " + regParam);
		double tol = model.getTol();
		System.out.println("Tol: " + tol);
		Double val = 8.0;
		Vector features = Vectors.dense(val);
		double p = model.predict(features);
		System.out.println("Prediction for " + val + " is " + p);

		System.out.println(8 * regParam + intersect);

	}
}

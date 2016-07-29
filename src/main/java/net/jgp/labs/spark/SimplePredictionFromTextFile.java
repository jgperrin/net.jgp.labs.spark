package net.jgp.labs.spark;

import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.jgp.labs.spark.udf.VectorBuilder;

public class SimplePredictionFromTextFile  {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		SimplePredictionFromTextFile app = new SimplePredictionFromTextFile();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("Simple prediction from Text File").master("local").getOrCreate();

		spark.udf().register("vectorBuilder", new VectorBuilder(), new VectorUDT());

		String filename = "data/tuple-data-file.csv";
		StructType schema = new StructType(
				new StructField[] { new StructField("_c0", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("_c1", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("features", new VectorUDT(), true, Metadata.empty()), });

		Dataset<Row> df = spark.read().format("csv").schema(schema).option("header", "false")
				.load(filename);
		df = df.withColumn("valuefeatures", df.col("_c0")).drop("_c0");
		df = df.withColumn("label", df.col("_c1")).drop("_c1");
		df.printSchema();
		// df.show();

		df = df.withColumn("features", callUDF("vectorBuilder", df.col("valuefeatures")));
		df.printSchema();
		df.show();

		LinearRegression lr = new LinearRegression().setMaxIter(20);//.setRegParam(1).setElasticNetParam(1);
		

		// Fit the model to the data.
		LinearRegressionModel model = lr.fit(df);

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

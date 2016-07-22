package net.jgp.labs.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class FirstPrediction {

	public static void main(String[] args) {
		FirstPrediction fp = new FirstPrediction();
		fp.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("First Prediction").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		System.out.println("Hello, Spark v." + sc.version());

		StructType schema = new StructType(
				new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("features", new VectorUDT(), false, Metadata.empty()), });
	//	DataFrame df = jsql.createDataFrame(data, schema);

	//	DataFrame df = sc.
		
		// Set parameters for the algorithm.
		// Here, we limit the number of iterations to 10.
//		LogisticRegression lr = new LogisticRegression().setMaxIter(10);

		// Fit the model to the data.
	//	LogisticRegressionModel model = lr.fit(df);

		// Inspect the model: get the feature weights.
		// Vector weights = model.weights();

		// Given a dataset, predict each point's label, and show the results.
	//	model.transform(df).show();
	}

}

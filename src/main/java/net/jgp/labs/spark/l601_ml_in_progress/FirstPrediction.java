package net.jgp.labs.spark.l600_ml;

import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FirstPrediction {

	public static void main(String[] args) {
		FirstPrediction fp = new FirstPrediction();
		fp.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("First Prediction").master("local").getOrCreate();

		StructType schema = new StructType(
				new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("features", new VectorUDT(), false, Metadata.empty()), });

		// TODO this example is not working yet
	}

}

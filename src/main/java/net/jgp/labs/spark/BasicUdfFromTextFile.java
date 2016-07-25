package net.jgp.labs.spark;

import static org.apache.spark.sql.functions.callUDF;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class BasicUdfFromTextFile implements Serializable {
	private static final long serialVersionUID = 3492970200940899011L;

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		BasicUdfFromTextFile app = new BasicUdfFromTextFile();
		app.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("Basic UDF from Text File").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		sqlContext.udf().register("x2Multiplier", new UDF1<Integer, Integer>() {
			private static final long serialVersionUID = -5372447039252716846L;

			@Override
			public Integer call(Integer x) {
				return x * 2;
			}
		}, DataTypes.IntegerType);

		String filename = "data/tuple-data-file.csv";
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "false").load(filename);
		df = df.withColumn("label", df.col("C0")).drop("C0");
		df = df.withColumn("value", df.col("C1")).drop("C1");
		df = df.withColumn("x2", callUDF("x2Multiplier", df.col("value").cast(DataTypes.IntegerType)));
		df.show();
	}
}


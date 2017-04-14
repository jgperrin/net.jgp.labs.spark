package net.jgp.labs.spark.l500_misc;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.labs.spark.x.datasource.CharCounterRelation;

/**
 * 
 * Inspired by http://stackoverflow.com/questions/39967194/creating-a-simple-1-row-spark-dataframe-with-java-api.
 * 
 * @author jgp
 *
 */
public class BuildDataFrameFromScratch {
	private static Logger log = LoggerFactory.getLogger(BuildDataFrameFromScratch.class);

	public static void main(String[] args) {
		BuildDataFrameFromScratch app = new BuildDataFrameFromScratch();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("Build a DataFrame from Scratch").master("local[*]")
				.getOrCreate();

		List<String> stringAsList = new ArrayList<String>();
		stringAsList.add("buzz");

		SparkContext sparkContext = spark.sparkContext();
		JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String row) -> {
			return RowFactory.create(row);
		});

		StructType schema = DataTypes.createStructType(
				new StructField[] { DataTypes.createStructField("fizz", DataTypes.StringType, false) });

		DataFrame df = sqlContext.createDataFrame(rowRDD, schema).toDF();
		df.show();
	}
}

package net.jgp.labs.spark.l500_misc;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Inspired by:
 * http://stackoverflow.com/questions/39967194/creating-a-simple-1-row-spark-dataframe-with-java-api.
 * 
 * @author jgp
 *
 */
public class BuildDataFrameFromScratch2 {
	private static transient Logger log = LoggerFactory.getLogger(BuildDataFrameFromScratch2.class);

	public static void main(String[] args) {
		BuildDataFrameFromScratch2 app = new BuildDataFrameFromScratch2();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("Build a DataFrame from Scratch").master("local[*]")
				.getOrCreate();

		List<String[]> stringAsList = new ArrayList<>();
		stringAsList.add(new String[] { "bar1.1", "bar2.1" });
		stringAsList.add(new String[] { "bar1.2", "bar2.2" });

		JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String[] row) -> RowFactory.create(row));

		// Creates schema
		StructType schema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("foe1", DataTypes.StringType, false),
						DataTypes.createStructField("foe2", DataTypes.StringType, false) });

		Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();

		log.debug("** Schema: ");
		df.printSchema();

		log.debug("** Data: ");
		df.show();

		sparkContext.close();
	}
}

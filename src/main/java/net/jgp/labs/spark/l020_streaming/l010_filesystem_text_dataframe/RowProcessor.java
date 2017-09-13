package net.jgp.labs.spark.l020_streaming.l010_filesystem_text_dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.jgp.labs.spark.x.utils.streaming.JavaSparkSessionSingleton;

public class RowProcessor implements VoidFunction<JavaRDD<String>> {
	private static final long serialVersionUID = 1863623004244123190L;

	@Override
	public void call(JavaRDD<String> rdd) throws Exception {

		JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
			private static final long serialVersionUID = 5167089361335095997L;

			@Override
			public Row call(String msg) {
				Row row = RowFactory.create(msg);
				return row;
			}
		});
		// Create Schema
		StructType schema = DataTypes.createStructType(
				new StructField[] { DataTypes.createStructField("Message", DataTypes.StringType, true) });

		// Get Spark 2.0 session
		SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
		Dataset<Row> msgDataFrame = spark.createDataFrame(rowRDD, schema);
		msgDataFrame.show();
	}

}

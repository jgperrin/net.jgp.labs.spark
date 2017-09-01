package net.jgp.labs.spark.l020_streaming.l010_filesystem_text_dataframe;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

public class StreamingIngestionFileSystemTextFileToDataframeApp implements Serializable {
	private static final long serialVersionUID = 6795623748995704732L;

	public static void main(String[] args) {
		StreamingIngestionFileSystemTextFileToDataframeApp app = new StreamingIngestionFileSystemTextFileToDataframeApp();
		app.start();
	}

	private void start() {
		// Create a local StreamingContext with two working thread and batch interval of
		// 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		JavaDStream<String> msgDataStream = jssc.textFileStream("file:\\tmp\\streaming\\in\\");
		msgDataStream.print();
		// Create JavaRDD<Row>
		msgDataStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = -590010339928376829L;

			@Override
			public void call(JavaRDD<String> rdd) {
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
		});

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class JavaSparkSessionSingleton {
	private static transient SparkSession instance = null;

	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession.builder().config(sparkConf).getOrCreate();
		}
		return instance;
	}
}

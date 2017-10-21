package net.jgp.labs.spark.l040_structured_streaming.l021_read_lines_multiple_file_streams_in_progress;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.labs.spark.x.utils.streaming.StreamingUtils;

public class ReadLinesFromMultipleFileStreams {
	private static transient Logger log = LoggerFactory.getLogger(ReadLinesFromMultipleFileStreams.class);

	public static void main(String[] args) {
		ReadLinesFromMultipleFileStreams app = new ReadLinesFromMultipleFileStreams();
		app.start();
	}

	private void start() {
		log.debug("-> start()");

		SparkSession spark = SparkSession.builder().appName("Read lines over a file stream").master("local")
				.getOrCreate();

		// @formatter:off
		Dataset<Row> df = spark
				.readStream()
				.format("text")
				.load(StreamingUtils.getInputDirectory());
		// @formatter:on

		StreamingQuery query = df.writeStream().outputMode(OutputMode.Update()).format("console").start();

		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			log.error("Exception while waiting for query to end {}.", e.getMessage(), e);
		}

		// In this case everything is a string
		df.show();
		df.printSchema();
	}
}

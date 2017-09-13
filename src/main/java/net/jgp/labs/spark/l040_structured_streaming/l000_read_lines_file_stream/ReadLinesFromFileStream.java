package net.jgp.labs.spark.l040_structured_streaming.l000_read_lines_file_stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.labs.spark.x.utils.streaming.StreamingUtils;

public class ReadLinesFromFileStream {
	private static Logger log = LoggerFactory.getLogger(ReadLinesFromFileStream.class);

	public static void main(String[] args) {
		ReadLinesFromFileStream app = new ReadLinesFromFileStream();
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

		// Never executed
		df.show();
		df.printSchema();
	}
}

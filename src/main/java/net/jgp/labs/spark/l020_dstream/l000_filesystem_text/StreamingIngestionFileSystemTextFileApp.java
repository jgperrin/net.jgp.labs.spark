package net.jgp.labs.spark.l020_dstream.l000_filesystem_text;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.jgp.labs.spark.x.utils.streaming.StreamingUtils;

public class StreamingIngestionFileSystemTextFileApp implements Serializable {
	private static final long serialVersionUID = 6795623748995704732L;

	public static void main(String[] args) {
		StreamingUtils.createInputDirectory();
		StreamingIngestionFileSystemTextFileApp app = new StreamingIngestionFileSystemTextFileApp();
		app.start();
	}

	private void start() {
		// Create a local StreamingContext with two working thread and batch interval of
		// 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		JavaDStream<String> msgDataStream = jssc.textFileStream(StreamingUtils.getInputDirectory());
		msgDataStream.print();

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

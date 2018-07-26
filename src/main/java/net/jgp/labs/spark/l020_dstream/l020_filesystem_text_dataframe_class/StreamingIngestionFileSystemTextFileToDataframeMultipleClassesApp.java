package net.jgp.labs.spark.l020_dstream.l020_filesystem_text_dataframe_class;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.jgp.labs.spark.x.utils.streaming.StreamingUtils;

public class StreamingIngestionFileSystemTextFileToDataframeMultipleClassesApp
    implements Serializable {
  private static final long serialVersionUID = 6795623748995704732L;

  public static void main(String[] args) {
    StreamingIngestionFileSystemTextFileToDataframeMultipleClassesApp app =
        new StreamingIngestionFileSystemTextFileToDataframeMultipleClassesApp();
    app.start();
  }

  private void start() {
    // Create a local StreamingContext with two working thread and batch
    // interval of
    // 1 second
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
        "Streaming Ingestion File System Text File to Dataframe");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations
        .seconds(5));

    JavaDStream<String> msgDataStream = jssc.textFileStream(StreamingUtils
        .getInputDirectory());

    msgDataStream.print();
    // Create JavaRDD<Row>
    msgDataStream.foreachRDD(new RowProcessor());

    jssc.start();
    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}

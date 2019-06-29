package net.jgp.labs.spark.l100_checkpoint;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Illustration of a simple non-eager checkpoint on a dataframe.
 * 
 * @author jgp
 */
public class DataframeCheckpointApp {
  public static void main(String[] args) {
    DataframeCheckpointApp app = new DataframeCheckpointApp();
    app.start();
  }

  private void start() {
    SparkConf conf = new SparkConf()
        .setAppName("Checkpoint")
        .setMaster("local[*]");
    SparkContext sparkContext = new SparkContext(conf);

    // We need to specify where Spark will save the checkpoint file. It can
    // be
    // an HDFS location.
    sparkContext.setCheckpointDir("/tmp");
    SparkSession spark = SparkSession.builder()
        .appName("Checkpoint")
        .master("local[*]")
        .getOrCreate();

    String filename = "data/tuple-data-file.csv";
    Dataset<Row> df1 =
        spark.read().format("csv").option("inferSchema", "true")
            .option("header", "false")
            .load(filename);
    System.out.println("DF #1 - step #1: simple dump of the dataframe");
    df1.show();

    System.out.println("DF #2 - step #2: same as DF #1 - step #1");
    Dataset<Row> df2 = df1.checkpoint(false);
    df2.show();

    df1 = df1.withColumn("x", df1.col("_c0"));
    System.out.println(
        "DF #1 - step #2: new column x, which is the same as _c0");
    df1.show();

    System.out.println("DF #2 - step #2: no operation was done on df2");
    df2.show();
  }
}
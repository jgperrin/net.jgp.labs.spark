package net.jgp.labs.spark.l000_ingestion.l100_json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A basic example of JSON conversion and simple manipulation on it.
 * 
 * @author jgp
 */
public class JsonToDataset {

  public static void main(String[] args) {
    System.out.println("Working directory = " + System.getProperty("user.dir"));
    JsonToDataset app = new JsonToDataset();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder().appName("JSON to Dataset")
        .master("local").getOrCreate();

    String filename = "data/north-carolina-school-performance-data.json";
    long start = System.currentTimeMillis();
    Dataset<Row> df = spark.read().json(filename);
    long stop = System.currentTimeMillis();
    System.out.println("Processing took " + (stop - start) + " ms");
    df.show();
    df.printSchema();

    // Flatenization
    df = df.withColumn("district", df.col("fields.district"));
    df = df.drop(df.col("fields.district")); // this does not work as the column
                                             // stays here (Spark 2.0.0)
    df.show();
    df.printSchema();
  }
}

package net.jgp.labs.spark.l000_ingestion.l100_json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A basic example of complex JSON array.
 * 
 * Note: The array is valid, but Spark does not like it
 * 
 * @author jgp
 */
public class JsonComplexArrayToDataset {

  public static void main(String[] args) {
    System.out.println("Working directory = " + System.getProperty("user.dir"));
    JsonComplexArrayToDataset app = new JsonComplexArrayToDataset();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder().appName(
        "Complex JSON array to Dataset").master("local").getOrCreate();

    String filename = "data/array-complex.json";
    long start = System.currentTimeMillis();
    Dataset<Row> df = spark.read().json(filename);
    long stop = System.currentTimeMillis();
    System.out.println("Processing took " + (stop - start) + " ms");
    df.show();
    df.printSchema();
  }
}

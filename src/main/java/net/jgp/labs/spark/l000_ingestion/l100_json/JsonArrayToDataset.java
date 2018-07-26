package net.jgp.labs.spark.l000_ingestion.l100_json;

import static org.apache.spark.sql.functions.explode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A basic example of JSON array.
 * 
 * Note: [100,500,600,700,800,200,900,300] is a valid array, but Spark does not
 * like it
 * 
 * @author jgp
 */
public class JsonArrayToDataset {

  public static void main(String[] args) {
    System.out.println("Working directory = " + System.getProperty("user.dir"));
    JsonArrayToDataset app = new JsonArrayToDataset();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder().appName("JSON array to Dataset")
        .master("local").getOrCreate();

    String filename = "data/array.json";
    long start = System.currentTimeMillis();
    Dataset<Row> df = spark.read().json(filename);
    long stop = System.currentTimeMillis();
    System.out.println("Processing took " + (stop - start) + " ms");
    df.show();
    df.printSchema();

    // Turns the "one liner" into a real column
    df = df.select(explode(df.col("valsInArrays"))).toDF("vals");
    df.show();
    df.printSchema();
  }
}

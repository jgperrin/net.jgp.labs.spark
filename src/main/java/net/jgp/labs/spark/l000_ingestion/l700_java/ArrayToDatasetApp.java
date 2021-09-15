package net.jgp.labs.spark.l000_ingestion.l700_java;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class ArrayToDatasetApp {

  public static void main(String[] args) {
    ArrayToDatasetApp app = new ArrayToDatasetApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Array to Dataset<String>")
        .master("local")
        .getOrCreate();

    String[] l = new String[] { "a", "b", "c", "d" };
    List<String> data = Arrays.asList(l);
    Dataset<String> df = spark.createDataset(data, Encoders.STRING());
    df.show();
    df.printSchema();
  }
}

package net.jgp.labs.spark.l000_ingestion.l000_csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDatasetApp {

  public static void main(String[] args) {
    System.out.println("Working directory = " + System.getProperty("user.dir"));
    CsvToDatasetApp app = new CsvToDatasetApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("CSV to Dataset")
        .master("local")
        .getOrCreate();

    String filename = "data/tuple-data-file.csv";
    Dataset<Row> df = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "false")
        .load(filename);
    df.show();
  }
}

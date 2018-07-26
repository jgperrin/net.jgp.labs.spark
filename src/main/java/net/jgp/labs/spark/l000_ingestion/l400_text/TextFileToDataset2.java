package net.jgp.labs.spark.l000_ingestion.l400_text;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TextFileToDataset2 {

  public static void main(String[] args) {
    System.out.println("Working directory = " + System.getProperty("user.dir"));
    TextFileToDataset2 app = new TextFileToDataset2();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Dataset from Text File")
        .master("local[*]")
        .getOrCreate();

    String filename = "data/simple-data-file.txt";
    Dataset<Row> df = spark.read().text(filename);
    df.show();
  }
}
